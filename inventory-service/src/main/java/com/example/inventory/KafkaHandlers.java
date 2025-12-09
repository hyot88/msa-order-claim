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

/**
 * 재고 서비스 Kafka 이벤트 핸들러
 *
 * <h3>주요 역할:</h3>
 * 주문 Saga의 첫 번째 단계로, OrderCreated 이벤트를 수신하여
 * 재고 확인/예약을 처리하고 결과를 inventory.events 토픽으로 발행합니다.
 *
 * <h3>Saga 흐름:</h3>
 * 1. OrderCreated 이벤트 수신 (order.events 토픽)
 * 2. 재고 확인 로직 실행 (시뮬레이션)
 * 3-a. 성공 시: InventoryReserved 이벤트 발행 → payment-service로 전달
 * 3-b. 실패 시: InventoryReservationFailed 이벤트 발행 → order-service가 주문 취소
 *
 * <h3>분산 추적:</h3>
 * Kafka 헤더에서 부모 Span 컨텍스트를 추출하고 새로운 자식 Span을 생성하여
 * 전체 주문 흐름을 Zipkin에서 추적할 수 있습니다.
 *
 * <h3>실패 시뮬레이션:</h3>
 * ServiceProperties.inventory 값(0.0~1.0)에 따라 확률적으로 재고 부족 시나리오를
 * 시뮬레이션합니다. (예: 0.2 = 20% 확률로 재고 부족 발생)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaHandlers {

    private final ObjectMapper om;
    private final KafkaTemplate<String, String> kafka;
    private final Tracer tracer; // Micrometer Tracing
    private final Propagator propagator; // 분산 추적 컨텍스트 전파
    private final ServiceProperties serviceProperties; // 실패율 설정

    /**
     * order.events 토픽에서 OrderCreated 이벤트를 수신하여 재고 처리
     *
     * <h3>처리 흐름:</h3>
     * 1. 분산 추적 컨텍스트 추출 및 Span 생성
     * 2. OrderCreated 이벤트 유효성 검증
     * 3. 재고 확인 (실제 시스템에서는 DB 조회 및 업데이트)
     * 4. 성공/실패에 따라 InventoryReserved 또는 InventoryReservationFailed 이벤트 발행
     * 5. 분산 추적 컨텍스트를 Kafka 헤더에 주입하여 다음 서비스로 전파
     *
     * @param rec Kafka 메시지 (key: orderId, value: OrderCreated JSON)
     * @throws Exception 처리 실패 시 (Kafka가 재시도)
     */
    @KafkaListener(topics = "order.events")
    public void onOrderEvents(ConsumerRecord<String, String> rec) throws Exception {
        io.micrometer.tracing.Span span = null;

        try {
            // 1단계: Kafka 헤더에서 분산 추적 컨텍스트 추출
            Map<String, String> carrier = new HashMap<>();
            rec.headers().forEach(header -> {
                String key = header.key();
                String value = new String(header.value(), StandardCharsets.UTF_8);
                carrier.put(key, value);
                log.debug("Header found: {}={}", key, value);
            });

            // 2단계: Propagator로 부모 Span 컨텍스트 복원
            Propagator.Getter<Map<String, String>> getter =
                (carrierMap, key) -> carrierMap.get(key);

            io.micrometer.tracing.Span.Builder spanBuilder = propagator.extract(carrier, getter);

            // 3단계: 자식 Span 생성 (부모가 있으면 연결, 없으면 새 trace 시작)
            if (spanBuilder != null) {
                span = spanBuilder
                        .name("inventory-process-order-event")
                        .tag("topic", rec.topic())
                        .tag("partition", String.valueOf(rec.partition()))
                        .tag("offset", String.valueOf(rec.offset()))
                        .start();
            } else {
                span = tracer.nextSpan()
                        .name("inventory-process-order-event")
                        .tag("topic", rec.topic())
                        .tag("partition", String.valueOf(rec.partition()))
                        .tag("offset", String.valueOf(rec.offset()))
                        .start();
            }

            // 4단계: Span 활성화 및 비즈니스 로직 실행
            try (io.micrometer.tracing.Tracer.SpanInScope ws = tracer.withSpan(span)) {
                log.info("[inventory] consume topic={} key={} value={}", rec.topic(), rec.key(), rec.value());

                // traceId 추출 (기존 방식 호환성)
                Header h = rec.headers().lastHeader("traceId");
                String traceId = h != null ? new String(h.value(), StandardCharsets.UTF_8) : span.context().traceId();

                // 5단계: 메시지 파싱 및 유효성 검증
                JsonNode node = om.readTree(rec.value());
                if (!node.has("orderId") || !node.has("userId") || !node.has("totalAmount")) {
                    log.debug("[inventory] ignore non-OrderCreated payload");
                    return;
                }

                UUID orderId = UUID.fromString(node.get("orderId").asText());
                span.tag("orderId", orderId.toString());

                // 6단계: 재고 확인 (시뮬레이션)
                // 실제 시스템: SELECT * FROM inventory WHERE product_id = ? FOR UPDATE
                // 현재: 설정된 확률로 랜덤 실패
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
