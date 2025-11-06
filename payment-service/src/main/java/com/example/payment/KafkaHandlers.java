package com.example.payment;

import com.example.payment.config.FailRateProperties;
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
import org.springframework.beans.factory.annotation.Value;
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
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper om;
    private final Tracer tracer;
    private final Propagator propagator;
    private final FailRateProperties failRateProperties;

    @KafkaListener(topics = "inventory.events", groupId = "payment")
    public void onInventoryEvents(ConsumerRecord<String, String> rec) throws Exception {
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
                        .name("payment-process-inventory-event")
                        .tag("topic", rec.topic())
                        .tag("partition", String.valueOf(rec.partition()))
                        .tag("offset", String.valueOf(rec.offset()))
                        .start();
            } else {
                // 부모 컨텍스트가 없는 경우 새 스팬 시작
                span = tracer.nextSpan()
                        .name("payment-process-inventory-event")
                        .tag("topic", rec.topic())
                        .tag("partition", String.valueOf(rec.partition()))
                        .tag("offset", String.valueOf(rec.offset()))
                        .start();
            }

            // 현재 스팬을 활성화
            try (io.micrometer.tracing.Tracer.SpanInScope ws = tracer.withSpan(span)) {
                log.info("[payment] consume topic={} key={} value={}", rec.topic(), rec.key(), rec.value());

                // 기존 traceId 헤더 추출 (호환성 유지)
                Header h = rec.headers().lastHeader("traceId");
                String traceId = h != null ? new String(h.value(), StandardCharsets.UTF_8) : span.context().traceId();

                JsonNode node = om.readTree(rec.value());
                if (!node.has("orderId")) return;

                UUID orderId = UUID.fromString(node.get("orderId").asText());
                span.tag("orderId", orderId.toString());

                boolean isFailure = node.has("reason"); // 간단 구분: reason 있으면 실패
                span.tag("inventoryStatus", isFailure ? "failed" : "success");

                if (isFailure) {
                    // 보상 플로우: 결제 시도 안하고 실패 이벤트 전달(또는 PaymentCancelled)
                    var evt = Map.of("orderId", orderId.toString(), "failedAt", Instant.now().toString(), "reason", "INVENTORY_FAIL");
                    String payload = om.writeValueAsString(evt);

                    ProducerRecord<String, String> out = new ProducerRecord<>("payment.events", orderId.toString(), payload);

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
                            log.error("[payment] publish payment.events FAILED orderId={}", orderId, ex);
                        } else {
                            log.info("[payment] publish payment.events OK partition={} offset={}",
                                    md.getRecordMetadata().partition(), md.getRecordMetadata().offset());
                        }
                    });
                    return;
                }

                // 결제 관리 시스템에서 결제 실패 상황을 인위적으로 시뮬레이션하기 위해 사용
                boolean fail = ThreadLocalRandom.current().nextDouble() < failRateProperties.getPayment();
                span.tag("paymentCheck", fail ? "failed" : "success");

                Map<String, Object> evt;
                if (!fail) {
                    evt = Map.of("orderId", orderId.toString(), "authorizedAt", Instant.now().toString());
                } else {
                    evt = Map.of("orderId", orderId.toString(), "failedAt", Instant.now().toString(), "reason", "PAYMENT_DECLINED");
                }

                String payload = om.writeValueAsString(evt);
                ProducerRecord<String, String> out = new ProducerRecord<>("payment.events", orderId.toString(), payload);

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
                        log.error("[payment] publish payment.events FAILED orderId={}", orderId, ex);
                    } else {
                        log.info("[payment] publish payment.events OK partition={} offset={}",
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
