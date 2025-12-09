package com.example.payment;

import com.example.payment.config.ServiceProperties;
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
 * 결제 서비스 Kafka 이벤트 핸들러
 *
 * <h3>주요 역할:</h3>
 * 주문 Saga의 두 번째 단계로, InventoryReserved 이벤트를 수신하여
 * 결제 처리를 수행하고 결과를 payment.events 토픽으로 발행합니다.
 *
 * <h3>Saga 흐름:</h3>
 * 1. InventoryReserved 또는 InventoryReservationFailed 이벤트 수신 (inventory.events)
 * 2-a. 재고 예약 실패: PaymentFailed 즉시 발행 (보상 트랜잭션)
 * 2-b. 재고 예약 성공: 결제 처리 로직 실행
 * 3-a. 결제 성공: PaymentAuthorized 발행 → order-service가 주문 승인
 * 3-b. 결제 실패: PaymentFailed 발행 → order-service가 주문 취소
 *
 * <h3>보상 트랜잭션 (Saga Compensation):</h3>
 * 재고 예약은 성공했지만 결제가 실패하면, PaymentFailed 이벤트가 발행되어
 * order-service가 주문을 취소하고 inventory-service에 재고 해제를 요청하게 됩니다.
 *
 * <h3>실패 시뮬레이션:</h3>
 * ServiceProperties.payment 값(0.0~1.0)에 따라 확률적으로 결제 실패 시나리오를
 * 시뮬레이션합니다. (예: 0.1 = 10% 확률로 결제 거절)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaHandlers {
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper om;
    private final Tracer tracer; // Micrometer Tracing
    private final Propagator propagator; // 분산 추적 컨텍스트 전파
    private final ServiceProperties serviceProperties; // 실패율 설정

    /**
     * inventory.events 토픽에서 재고 예약 결과를 수신하여 결제 처리
     *
     * <h3>처리 흐름:</h3>
     * 1. 분산 추적 컨텍스트 추출 및 Span 생성
     * 2. 재고 예약 성공/실패 판단 (reason 필드 유무로 구분)
     * 3-a. 재고 실패: 즉시 PaymentFailed 발행 (보상 트랜잭션)
     * 3-b. 재고 성공: 결제 처리 후 PaymentAuthorized 또는 PaymentFailed 발행
     * 4. 분산 추적 컨텍스트를 Kafka 헤더에 주입
     *
     * @param rec Kafka 메시지 (key: orderId, value: InventoryReserved/Failed JSON)
     * @throws Exception 처리 실패 시 (Kafka가 재시도)
     */
    @KafkaListener(topics = "inventory.events", groupId = "payment")
    public void onInventoryEvents(ConsumerRecord<String, String> rec) throws Exception {
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

            // 3단계: 자식 Span 생성
            if (spanBuilder != null) {
                span = spanBuilder
                        .name("payment-process-inventory-event")
                        .tag("topic", rec.topic())
                        .tag("partition", String.valueOf(rec.partition()))
                        .tag("offset", String.valueOf(rec.offset()))
                        .start();
            } else {
                span = tracer.nextSpan()
                        .name("payment-process-inventory-event")
                        .tag("topic", rec.topic())
                        .tag("partition", String.valueOf(rec.partition()))
                        .tag("offset", String.valueOf(rec.offset()))
                        .start();
            }

            // 4단계: Span 활성화 및 비즈니스 로직 실행
            try (io.micrometer.tracing.Tracer.SpanInScope ws = tracer.withSpan(span)) {
                log.info("[payment] consume topic={} key={} value={}", rec.topic(), rec.key(), rec.value());

                // traceId 추출 (기존 방식 호환성)
                Header h = rec.headers().lastHeader("traceId");
                String traceId = h != null ? new String(h.value(), StandardCharsets.UTF_8) : span.context().traceId();

                // 5단계: 메시지 파싱
                JsonNode node = om.readTree(rec.value());
                if (!node.has("orderId")) return;

                UUID orderId = UUID.fromString(node.get("orderId").asText());
                span.tag("orderId", orderId.toString());

                // 6단계: 재고 예약 실패 여부 판단 (reason 필드로 구분)
                boolean isFailure = node.has("reason");
                span.tag("inventoryStatus", isFailure ? "failed" : "success");

                if (isFailure) {
                    // 보상 트랜잭션: 재고 예약 실패 시 즉시 PaymentFailed 발행
                    // order-service가 이 이벤트를 받아 주문을 취소합니다
                    var evt = Map.of("orderId", orderId.toString(), "failedAt", Instant.now().toString(), "reason", "INVENTORY_FAIL");
                    String payload = om.writeValueAsString(evt);

                    ProducerRecord<String, String> out = new ProducerRecord<>("payment.events", orderId.toString(), payload);

                    // 분산 추적 컨텍스트 주입
                    Map<String, String> outgoingCarrier = new HashMap<>();
                    propagator.inject(span.context(), outgoingCarrier,
                            (carrierMap, key, value) -> carrierMap.put(key, value));

                    for (Map.Entry<String, String> entry : outgoingCarrier.entrySet()) {
                        out.headers().add(new RecordHeader(entry.getKey(), entry.getValue().getBytes(StandardCharsets.UTF_8)));
                    }

                    // 이전 방식의 traceId 헤더도 호환성 유지
                    out.headers().add(new RecordHeader("traceId", traceId.getBytes(StandardCharsets.UTF_8)));

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

                // 7단계: 결제 처리 (시뮬레이션)
                // 실제 시스템: PG사 API 호출 (카드 승인, 계좌 이체 등)
                // 현재: 설정된 확률로 랜덤 실패
                boolean fail = ThreadLocalRandom.current().nextDouble() < serviceProperties.getPayment();
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
