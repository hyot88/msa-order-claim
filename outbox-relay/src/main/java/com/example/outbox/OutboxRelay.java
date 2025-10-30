package com.example.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import lombok.RequiredArgsConstructor;
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

        // 3) Kafka 전송 & published=true
        for (OutboxEvent e : events) {
            // 부모 컨텍스트 추출 및 자식 스팬 시작
            io.micrometer.tracing.Span span = null;
            try {
                // 헤더에서 traceId 추출
                String traceId = getText(e.getHeaders(), "traceId");

                if (traceId != null) {
                    // 헤더에서 부모 컨텍스트 추출을 위한 캐리어 생성
                    Map<String, String> carrier = new HashMap<>();
                    // B3 형식으로 traceId 설정 (전체 컨텍스트는 아니지만 최소한 traceId는 연결)
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

                    // JsonNode -> String
                    String value = toJson(e.getPayload());

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

                    kafka.send(record).join();
                    e.setPublished(true); // dirty checking으로 UPDATE
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

