package com.example.outbox;

import com.example.common.outbox.OutboxEvent;
import com.example.common.outbox.OutboxRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Outbox 이벤트를 Kafka로 중계하는 핵심 컴포넌트
 *
 * <h3>Outbox 패턴 구현의 핵심:</h3>
 * 이 클래스는 Transactional Outbox 패턴의 "Relay" 역할을 담당합니다.
 * 서비스들이 데이터베이스에 저장한 이벤트를 주기적으로 폴링하여
 * Kafka로 발행함으로써 이벤트의 at-least-once 전달을 보장합니다.
 *
 * <h3>동작 흐름:</h3>
 * 1. 500ms마다 pump() 메서드가 자동 실행 (@Scheduled)
 * 2. outbox_event 테이블에서 published=false인 이벤트를 최대 50개 조회
 * 3. FOR UPDATE SKIP LOCKED로 다중 인스턴스 환경에서도 중복 처리 방지
 * 4. 각 이벤트를 적절한 Kafka 토픽으로 발행
 * 5. 발행 성공 시 published=true로 업데이트 (dirty checking)
 * 6. 트랜잭션 커밋 시점에 DB 업데이트 일괄 반영
 *
 * <h3>분산 추적 (Distributed Tracing):</h3>
 * - OutboxEvent의 headers에서 부모 Span 컨텍스트를 복원
 * - 새로운 자식 Span을 생성하여 Kafka 헤더에 주입
 * - Zipkin에서 order-service → outbox-relay → kafka → consumer로 이어지는 흐름 추적 가능
 *
 * <h3>이벤트 라우팅:</h3>
 * - OrderCreated, OrderApproved, OrderCancelled → "order.events" 토픽
 * - 향후 다른 이벤트 타입 추가 시 switch 문에서 라우팅 로직 확장
 *
 * <h3>다중 인스턴스 지원:</h3>
 * outbox-relay를 여러 인스턴스로 실행해도 FOR UPDATE SKIP LOCKED 덕분에
 * 각 이벤트는 정확히 한 인스턴스에서만 처리됩니다.
 *
 * <h3>에러 처리:</h3>
 * - Kafka 발행 실패 시 published=false로 유지되어 다음 폴링에서 재시도
 * - 트랜잭션 롤백을 방지하기 위해 예외를 잡아서 로깅만 수행
 */
@Component
@EnableScheduling
@RequiredArgsConstructor
@Slf4j
public class OutboxRelay {

    private final OutboxRepository repo;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper om;

    private final Tracer tracer; // Micrometer Tracing - Span 생성 및 관리
    private final Propagator propagator; // 분산 추적 컨텍스트 전파 (B3, W3C)

    /** 한 번에 처리할 최대 이벤트 수 (기본값: 50) */
    @Value("${relay.batchSize:50}") int batchSize;

    /**
     * 미발행 이벤트를 주기적으로 폴링하여 Kafka로 발행
     *
     * <h3>실행 주기:</h3>
     * fixedDelay = 500ms (이전 실행이 완료된 후 500ms 대기)
     *
     * <h3>@Transactional의 역할:</h3>
     * - lockAndFetchRaw()가 FOR UPDATE로 행 잠금
     * - published 플래그 업데이트 (dirty checking)
     * - 트랜잭션 커밋 시 모든 변경사항 일괄 반영
     *
     * <h3>처리 단계:</h3>
     * 1. DB 조회: published=false인 이벤트를 batchSize만큼 조회 (행 잠금)
     * 2. 분산 추적: OutboxEvent.headers에서 부모 Span 컨텍스트 복원
     * 3. 토픽 결정: 이벤트 타입에 따라 적절한 Kafka 토픽 선택
     * 4. Kafka 발행: 페이로드와 헤더(tracing context 포함)를 Kafka로 전송
     * 5. 상태 업데이트: published=true로 변경 (dirty checking)
     * 6. 트랜잭션 커밋: DB 업데이트 일괄 반영
     */
    @Scheduled(fixedDelayString = "${relay.pollMs:500}")
    @Transactional
    public void pump() {
        // 1단계: 미발행 이벤트 조회 (FOR UPDATE SKIP LOCKED)
        List<OutboxEvent> events = repo.lockAndFetchRaw(batchSize);
        if (events.isEmpty()) return;

        log.info("Found {} unpublished events to process", events.size());

        // 3) Kafka 전송 & published=true
        for (OutboxEvent e : events) {
            // 부모 컨텍스트 추출 및 자식 스팬 시작
            io.micrometer.tracing.Span span = null;
            try {
                // 헤더에서 traceId 추출 (이전 방식과의 호환성 유지)
                String traceId = getText(e.getHeaders(), "traceId");
                log.debug("Event type: {}, aggregateId: {}, traceId: {}", e.getType(), e.getAggregateId(), traceId);

                if (traceId != null) {
                    // 헤더에서 부모 컨텍스트 추출을 위한 캐리어 생성
                    Map<String, String> carrier = new HashMap<>();

                    // 헤더의 모든 필드를 캐리어에 복사
                    if (e.getHeaders() != null) {
                        e.getHeaders().fieldNames().forEachRemaining(fieldName -> {
                            String value = getText(e.getHeaders(), fieldName);
                            if (value != null) {
                                carrier.put(fieldName, value);
                                log.debug("Added header to carrier: {}={}", fieldName, value);
                            }
                        });
                    }

                    // 이전 방식과의 호환성을 위해 b3 헤더에도 추가
                    carrier.put("b3", traceId);

                    // propagator를 사용하여 부모 컨텍스트 추출
                    io.micrometer.tracing.propagation.Propagator.Getter<Map<String, String>> getter =
                            (carrier1, key) -> carrier1.get(key);
                    io.micrometer.tracing.Span.Builder spanBuilder = propagator.extract(carrier, getter);

                    // 자식 스팬 시작
                    if (spanBuilder != null) {
                        span = spanBuilder
                                .name("outbox-relay-process-" + e.getType())
                                .tag("aggregateId", e.getAggregateId())
                                .tag("eventType", e.getType())
                                .start();
                    } else {
                        span = tracer.nextSpan()
                                .name("outbox-relay-process-" + e.getType())
                                .tag("aggregateId", e.getAggregateId())
                                .tag("eventType", e.getType())
                                .start();
                    }
                } else {
                    // traceId가 없는 경우 새 스팬 시작
                    span = tracer.nextSpan()
                            .name("outbox-relay-process-" + e.getType())
                            .tag("aggregateId", e.getAggregateId())
                            .tag("eventType", e.getType())
                            .start();
                }

                // 현재 스팬을 활성화
                try (io.micrometer.tracing.Tracer.SpanInScope ws = tracer.withSpan(span)) {
                    String topic = switch (e.getType()) {
                        case "OrderCreated", "OrderApproved", "OrderCancelled" -> "order.events";
                        default -> "order.events";
                    };
                    log.info("Determined topic for event type {}: {}", e.getType(), topic);

                    // JsonNode -> String
                    String value = toJson(e.getPayload());
                    log.debug("Message payload: {}", value);

                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, e.getAggregateId(), value);
                    record.headers()
                            .add(new RecordHeader("eventType", bytes(e.getType())));

                    // 현재 스팬의 컨텍스트를 Kafka 헤더에 주입
                    Map<String, String> kafkaHeaderCarrier = new HashMap<>();
                    propagator.inject(span.context(), kafkaHeaderCarrier,
                            (carrier1, key, value1) -> carrier1.put(key, value1));

                    // 주입된 헤더를 Kafka 레코드에 추가
                    for (Map.Entry<String, String> entry : kafkaHeaderCarrier.entrySet()) {
                        record.headers().add(new RecordHeader(entry.getKey(), bytes(entry.getValue())));
                    }

                    try {
                        log.info("Sending message to topic: {}, key: {}, type: {}", topic, e.getAggregateId(), e.getType());
                        kafka.send(record).join(); // Wait for completion
                        log.info("Successfully sent message to topic: {}, key: {}", topic, e.getAggregateId());
                        e.setPublished(true); // dirty checking으로 UPDATE
                    } catch (Exception ex) {
                        log.error("Exception during Kafka send operation. Topic: {}, Key: {}, Type: {}", 
                                topic, e.getAggregateId(), e.getType(), ex);
                        // Don't rethrow to avoid transaction rollback
                    }
                }
            } finally {
                // 스팬 종료
                if (span != null) {
                    span.end();
                }
            }
        }

        // 4) 트랜잭션 커밋 시점에 일괄 flush
    }

    private String toJson(JsonNode node) {
        try {
            return node == null ? "{}" : om.writeValueAsString(node);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to serialize payload JsonNode", ex);
        }
    }
    private String getText(JsonNode node, String field) {
        return (node != null && node.hasNonNull(field)) ? node.get(field).asText() : null;
    }
    private static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }
}
