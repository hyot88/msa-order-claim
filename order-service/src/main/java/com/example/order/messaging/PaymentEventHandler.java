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

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventHandler {

    private final ObjectMapper om;
    private final OrderService orderService;
    private final ProcessedMessageRepository processedRepo;
    private final Tracer tracer;
    private final Propagator propagator;

    @KafkaListener(topics = "payment.events", groupId = "order")
    public void onPaymentEvents(ConsumerRecord<String, String> rec) throws Exception {
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
                        .name("order-process-payment-event")
                        .tag("topic", rec.topic())
                        .tag("partition", String.valueOf(rec.partition()))
                        .tag("offset", String.valueOf(rec.offset()))
                        .start();
            } else {
                // 부모 컨텍스트가 없는 경우 새 스팬 시작
                span = tracer.nextSpan()
                        .name("order-process-payment-event")
                        .tag("topic", rec.topic())
                        .tag("partition", String.valueOf(rec.partition()))
                        .tag("offset", String.valueOf(rec.offset()))
                        .start();
            }

            // 현재 스팬을 활성화
            try (io.micrometer.tracing.Tracer.SpanInScope ws = tracer.withSpan(span)) {
                log.info("[order] consume topic={} key={} value={}", rec.topic(), rec.key(), rec.value());

                // 멱등 체크: (topic, partition, offset) 유니크
                try {
                    processedRepo.save(ProcessedMessage.builder()
                            .topic(rec.topic())
                            .partitionId(rec.partition())
                            .offset(rec.offset())
                            .messageKey(rec.key())
                            .build());
                } catch (DataIntegrityViolationException dup) {
                    // 이미 처리한 메시지 → 아무 것도 하지 않음
                    log.debug("[order] Duplicate message detected, skipping processing");
                    return;
                }

                JsonNode node = om.readTree(rec.value());
                if (!node.has("orderId")) {
                    log.debug("[order] Message does not contain orderId, skipping");
                    return;
                }

                UUID orderId = UUID.fromString(node.get("orderId").asText());
                span.tag("orderId", orderId.toString());

                boolean isFailure = node.has("reason");
                span.tag("paymentStatus", isFailure ? "failed" : "success");

                if (isFailure) {
                    String reason = node.get("reason").asText();
                    span.tag("failureReason", reason);
                    log.info("[order] Processing payment failure for orderId={}, reason={}", orderId, reason);
                    orderService.cancel(orderId, reason);
                } else {
                    log.info("[order] Processing payment success for orderId={}", orderId);
                    orderService.approve(orderId);
                }
            }
        } catch (Exception e) {
            if (span != null) {
                span.error(e);
            }
            log.error("[order] Error processing payment event", e);
            throw e;
        } finally {
            // 스팬 종료
            if (span != null) {
                span.end();
            }
        }
    }
}
