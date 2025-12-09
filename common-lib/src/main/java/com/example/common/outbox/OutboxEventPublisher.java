package com.example.common.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Outbox 이벤트 발행을 담당하는 퍼블리셔
 *
 * <h3>주요 역할:</h3>
 * 1. 도메인 이벤트를 OutboxEvent 엔티티로 변환하여 데이터베이스에 저장
 * 2. 분산 추적(Distributed Tracing)을 위한 traceId 및 컨텍스트 전파
 * 3. 이벤트 중복 방지를 위한 페이로드 해시 생성
 *
 * <h3>사용 예시:</h3>
 * <pre>
 * {@code
 * @Transactional
 * public void createOrder(...) {
 *     Order order = orderRepository.save(new Order(...));
 *     outboxEventPublisher.publish(
 *         "Order",
 *         order.getId().toString(),
 *         "OrderCreated",
 *         new OrderCreated(...),
 *         "order-service"
 *     );
 * }
 * }
 * </pre>
 *
 * <h3>중요:</h3>
 * 이 메서드는 반드시 @Transactional 컨텍스트 내에서 호출되어야 합니다.
 * 도메인 엔티티 저장과 OutboxEvent 저장이 동일한 트랜잭션으로 묶여야
 * Outbox 패턴의 원자성이 보장됩니다.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxEventPublisher {
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final Tracer tracer; // Micrometer Tracing - 현재 스팬(Span) 추적
    private final Propagator propagator; // 분산 추적 컨텍스트 전파

    /**
     * 이벤트를 Outbox 테이블에 발행
     *
     * @param aggregateType 애그리게이트 타입 (예: "Order")
     * @param aggregateId   애그리게이트 ID (예: 주문 ID)
     * @param eventType     이벤트 타입 (예: "OrderCreated")
     * @param event         이벤트 객체 (JSON으로 직렬화됨)
     * @param eventSource   이벤트 발행 서비스명 (예: "order-service")
     * @throws OutboxPublishException 이벤트 발행 실패 시
     */
    public void publish(String aggregateType, String aggregateId, String eventType, Object event, String eventSource) {
        try {
            log.info("Publishing outbox event: type={}, aggregateId={}", eventType, aggregateId);

            // 1. 이벤트 객체를 JSON으로 직렬화
            JsonNode payload = objectMapper.valueToTree(event);

            // 2. 분산 추적 컨텍스트 추출 (Zipkin 등에서 요청 흐름 추적을 위해 필요)
            Map<String, String> headerMap = new HashMap<>();
            String traceId = Optional.ofNullable(tracer.currentSpan())
                    .map(span -> span.context().traceId())
                    .orElse(null);

            // 3. 현재 스팬의 컨텍스트를 헤더에 주입 (W3C Trace Context 형식)
            if (tracer.currentSpan() != null) {
                propagator.inject(tracer.currentSpan().context(), headerMap,
                        Map::put);
                log.debug("Injected trace context into headers: {}", headerMap);
            }

            // 4. 헤더 JSON 생성 (traceId, eventSource 및 전파된 컨텍스트 포함)
            ObjectNode headers = objectMapper.createObjectNode()
                    .put("traceId", traceId)
                    .put("eventSource", eventSource);

            for (Map.Entry<String, String> entry : headerMap.entrySet()) {
                headers.put(entry.getKey(), entry.getValue());
            }

            // 5. OutboxEvent 엔티티 생성 및 저장
            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .aggregateType(aggregateType)
                    .aggregateId(aggregateId)
                    .type(eventType)
                    .payload(payload)
                    .payloadHash(sha256(payload.toString())) // 중복 방지용 해시
                    .headers(headers)
                    .published(false) // outbox-relay가 이 이벤트를 Kafka로 발행할 예정
                    .build();

            outboxRepository.save(outboxEvent);
            log.info("Successfully published outbox event: type={}, aggregateId={}", eventType, aggregateId);
        } catch (Exception e) {
            log.error("Failed to publish outbox event: type={}, aggregateId={}", eventType, aggregateId, e);
            throw new OutboxPublishException("Failed to publish outbox event", e);
        }
    }

    /**
     * SHA-256 해시 생성 (이벤트 중복 방지용)
     *
     * 동일한 페이로드를 가진 이벤트가 중복 저장되는 것을 방지하기 위해 사용됩니다.
     * DB의 unique constraint (aggregate_id, type, payload_hash)가 중복을 차단합니다.
     *
     * @param input 해시할 문자열 (JSON 페이로드)
     * @return SHA-256 해시값 (16진수 문자열)
     */
    private String sha256(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new OutboxPublishException("Failed to calculate SHA-256 hash", e);
        }
    }
}
