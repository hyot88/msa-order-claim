package com.example.outbox;

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

@Component
@EnableScheduling
@RequiredArgsConstructor
@Slf4j
public class OutboxRelay {

    private final OutboxEventRepository repo;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper om;

    private final Tracer tracer;
    private final Propagator propagator;

    @Value("${relay.batchSize:50}") int batchSize;

    @Scheduled(fixedDelayString = "${relay.pollMs:500}")
    @Transactional
    public void pump() {
        // 1) unpublished 행들을 락걸고 가져오기
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
