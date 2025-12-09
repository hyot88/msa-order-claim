package com.example.order.messaging;

import com.example.order.idempotency.ProcessedMessage;
import com.example.order.idempotency.ProcessedMessageRepository;
import com.example.order.service.OrderService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 결제 이벤트를 수신하여 주문 상태를 업데이트하는 핸들러
 *
 * <h3>Saga 패턴의 핵심 컴포넌트:</h3>
 * 이 핸들러는 주문 Saga의 마지막 단계를 담당합니다:
 * 1. OrderCreated → inventory-service (재고 확인)
 * 2. InventoryReserved → payment-service (결제 처리)
 * 3. PaymentAuthorized → order-service ★ 이 핸들러가 처리 (주문 승인)
 *
 * <h3>처리 로직:</h3>
 * - PaymentAuthorized: 주문을 APPROVED 상태로 변경
 * - PaymentFailed: 주문을 CANCELLED 상태로 변경 (보상 트랜잭션)
 *
 * <h3>멱등성 보장:</h3>
 * ProcessedMessage 테이블에 (topic, partition, offset)을 unique 키로 저장하여
 * 동일한 메시지가 재처리되지 않도록 보장합니다.
 *
 * <h3>분산 추적:</h3>
 * Kafka 헤더에서 tracing context를 추출하여 Zipkin에서 전체 요청 흐름을 추적할 수 있습니다.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventHandler {

    private final ObjectMapper om;
    private final OrderService orderService;
    private final ProcessedMessageRepository processedRepo;
    private final Tracer tracer; // Micrometer Tracing - 현재 스팬 관리
    private final Propagator propagator; // 분산 추적 컨텍스트 전파

    /**
     * payment.events 토픽에서 결제 이벤트를 수신
     *
     * <h3>수신 이벤트:</h3>
     * - PaymentAuthorized: {"orderId": "...", "amount": 17500}
     * - PaymentFailed: {"orderId": "...", "reason": "insufficient funds"}
     *
     * <h3>처리 흐름:</h3>
     * 1. 분산 추적 컨텍스트 추출 (Kafka 헤더 → Span)
     * 2. 멱등성 검사 (ProcessedMessage 테이블 체크)
     * 3. 메시지 파싱 및 비즈니스 로직 실행
     *    - 성공: orderService.approve(orderId)
     *    - 실패: orderService.cancel(orderId, reason)
     *
     * @param rec Kafka 메시지 (key: orderId, value: JSON payload)
     * @throws Exception 처리 실패 시 (Kafka는 자동으로 재시도)
     */
    @KafkaListener(topics = "payment.events", groupId = "order")
    public void onPaymentEvents(ConsumerRecord<String, String> rec) throws Exception {
        io.micrometer.tracing.Span span = null;

        try {
            // 1. Kafka 헤더에서 분산 추적 컨텍스트 추출
            Map<String, String> carrier = new HashMap<>();
            rec.headers().forEach(header -> {
                String key = header.key();
                String value = new String(header.value(), StandardCharsets.UTF_8);
                carrier.put(key, value);
                log.debug("Header found: {}={}", key, value);
            });

            // 2. Propagator로 부모 Span 컨텍스트 복원
            Propagator.Getter<Map<String, String>> getter =
                (carrierMap, key) -> carrierMap.get(key);

            io.micrometer.tracing.Span.Builder spanBuilder = propagator.extract(carrier, getter);

            // 3. 자식 Span 생성 (Zipkin에서 연결된 흐름으로 표시됨)
            if (spanBuilder != null) {
                span = spanBuilder
                        .name("order-process-payment-event")
                        .tag("topic", rec.topic())
                        .tag("partition", String.valueOf(rec.partition()))
                        .tag("offset", String.valueOf(rec.offset()))
                        .start();
            } else {
                // 부모 컨텍스트가 없으면 새로운 trace 시작
                span = tracer.nextSpan()
                        .name("order-process-payment-event")
                        .tag("topic", rec.topic())
                        .tag("partition", String.valueOf(rec.partition()))
                        .tag("offset", String.valueOf(rec.offset()))
                        .start();
            }

            // 4. Span을 현재 컨텍스트에 활성화 (로그에 traceId 포함됨)
            try (io.micrometer.tracing.Tracer.SpanInScope ws = tracer.withSpan(span)) {
                log.info("[order] consume topic={} key={} value={}", rec.topic(), rec.key(), rec.value());

                // 5. 멱등성 체크: 동일한 메시지를 두 번 처리하지 않도록 보장
                // (topic, partition, offset)이 unique 키로 설정되어 중복 저장 시 예외 발생
                try {
                    processedRepo.save(ProcessedMessage.builder()
                            .topic(rec.topic())
                            .partitionId(rec.partition())
                            .offset(rec.offset())
                            .messageKey(rec.key())
                            .build());
                } catch (DataIntegrityViolationException dup) {
                    // 이미 처리한 메시지 → 중복 처리 방지
                    log.debug("[order] Duplicate message detected, skipping processing");
                    return;
                }

                // 6. 메시지 파싱 및 비즈니스 로직 실행
                JsonNode node = om.readTree(rec.value());
                if (!node.has("orderId")) {
                    log.debug("[order] Message does not contain orderId, skipping");
                    return;
                }

                UUID orderId = UUID.fromString(node.get("orderId").asText());
                span.tag("orderId", orderId.toString());

                // 7. 성공 vs 실패 판단 (reason 필드 존재 여부)
                boolean isFailure = node.has("reason");
                span.tag("paymentStatus", isFailure ? "failed" : "success");

                if (isFailure) {
                    // 결제 실패 → 주문 취소 (보상 트랜잭션)
                    String reason = node.get("reason").asText();
                    span.tag("failureReason", reason);
                    log.info("[order] Processing payment failure for orderId={}, reason={}", orderId, reason);
                    orderService.cancel(orderId, reason);
                } else {
                    // 결제 성공 → 주문 승인
                    log.info("[order] Processing payment success for orderId={}", orderId);
                    orderService.approve(orderId);
                }
            }
        } catch (Exception e) {
            if (span != null) {
                span.error(e); // Zipkin에 에러 정보 기록
            }
            log.error("[order] Error processing payment event", e);
            throw e; // Kafka가 재시도 처리
        } finally {
            if (span != null) {
                span.end(); // Span 종료 (duration 기록)
            }
        }
    }
}
