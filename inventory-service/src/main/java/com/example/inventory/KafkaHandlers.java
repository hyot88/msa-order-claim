package com.example.inventory;

import com.example.inventory.config.ServiceProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaHandlers {

    private final ObjectMapper om;
    private final KafkaTemplate<String, String> kafka;
    private final Tracer tracer;
    private final Propagator propagator;
    private final ServiceProperties serviceProperties;

    @KafkaListener(topics = "order.events") // 👈 containerFactory 지정 불필요(기본 bean 사용)
    public void onOrderEvents(ConsumerRecord<String, String> rec) throws Exception {
        // 스팬 생성을 위한 변수
        io.micrometer.tracing.Span span = null;

        try {
            // Kafka 헤더에서 트레이싱 컨텍스트 추출을 위한 캐리어 생성
            Map<String, String> carrier = new HashMap<>();

            // Kafka 헤더를 캐리어에 복사
            rec.headers().forEach(header -> {
                String key = header.key();
                String value = new String(header.value(), StandardCharsets.UTF_8);
                carrier.put(key, value);
                log.debug("Header found: {}={}", key, value);
            });

            // Propagator를 사용하여 부모 컨텍스트 추출
            Propagator.Getter<Map<String, String>> getter = 
                (carrierMap, key) -> carrierMap.get(key);

            io.micrometer.tracing.Span.Builder spanBuilder = propagator.extract(carrier, getter);

            // 자식 스팬 생성
            if (spanBuilder != null) {
                span = spanBuilder
                        .name("inventory-process-order-event")
                        .tag("topic", rec.topic())
                        .tag("partition", String.valueOf(rec.partition()))
                        .tag("offset", String.valueOf(rec.offset()))
                        .start();
            } else {
                // 부모 컨텍스트가 없는 경우 새 스팬 시작
                span = tracer.nextSpan()
                        .name("inventory-process-order-event")
                        .tag("topic", rec.topic())
                        .tag("partition", String.valueOf(rec.partition()))
                        .tag("offset", String.valueOf(rec.offset()))
                        .start();
            }

            // 현재 스팬을 활성화
            try (io.micrometer.tracing.Tracer.SpanInScope ws = tracer.withSpan(span)) {
                log.info("[inventory] consume topic={} key={} value={}", rec.topic(), rec.key(), rec.value());

                // 기존 traceId 헤더 추출 (호환성 유지)
                Header h = rec.headers().lastHeader("traceId");
                String traceId = h != null ? new String(h.value(), StandardCharsets.UTF_8) : span.context().traceId();

                JsonNode node = om.readTree(rec.value());
                // 우리가 발행한 OrderCreated 포맷인지 확인
                if (!node.has("orderId") || !node.has("userId") || !node.has("totalAmount")) {
                    log.debug("[inventory] ignore non-OrderCreated payload");
                    return;
                }

                UUID orderId = UUID.fromString(node.get("orderId").asText());
                span.tag("orderId", orderId.toString());

                // 재고 관리 시스템에서 재고 부족 상황을 인위적으로 시뮬레이션하기 위해 사용
                boolean fail = ThreadLocalRandom.current().nextDouble() < serviceProperties.getInventory();
                span.tag("inventoryCheck", fail ? "failed" : "success");

                Map<String, Object> evt = fail
                        ? Map.of("orderId", orderId.toString(), "reason", "OUT_OF_STOCK", "failedAt", Instant.now().toString())
                        : Map.of("orderId", orderId.toString(), "reservedAt", Instant.now().toString());

                String payload = om.writeValueAsString(evt);

                ProducerRecord<String, String> out = new ProducerRecord<>("inventory.events", orderId.toString(), payload);

                // 현재 스팬의 컨텍스트를 Kafka 헤더에 주입
                Map<String, String> outgoingCarrier = new HashMap<>();
                propagator.inject(span.context(), outgoingCarrier, 
                        (carrierMap, key, value) -> carrierMap.put(key, value));

                // 주입된 헤더를 Kafka 레코드에 추가
                for (Map.Entry<String, String> entry : outgoingCarrier.entrySet()) {
                    out.headers().add(new RecordHeader(entry.getKey(), entry.getValue().getBytes(StandardCharsets.UTF_8)));
                }

                // 이전 방식의 traceId 헤더도 호환성을 위해 유지
                out.headers().add(new RecordHeader("traceId", traceId.getBytes(StandardCharsets.UTF_8)));

                // Capture span in a final variable for use in lambda
                final io.micrometer.tracing.Span currentSpan = span;
                kafka.send(out).whenComplete((md, ex) -> {
                    if (ex != null) {
                        currentSpan.error(ex);
                        log.error("[inventory] publish inventory.events FAILED orderId={}", orderId, ex);
                    } else {
                        log.info("[inventory] publish inventory.events OK partition={} offset={}",
                                md.getRecordMetadata().partition(), md.getRecordMetadata().offset());
                    }
                });
            }
        } catch (Exception e) {
            if (span != null) {
                span.error(e);
            }
            throw e;
        } finally {
            // 스팬 종료
            if (span != null) {
                span.end();
            }
        }
    }
}
