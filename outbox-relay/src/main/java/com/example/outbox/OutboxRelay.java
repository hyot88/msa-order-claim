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
            String topic = switch (e.getType()) {
                case "OrderCreated", "OrderApproved", "OrderCancelled" -> "order.events";
                default -> "order.events";
            };

            // JsonNode -> String
            String value = toJson(e.getPayload());

            // headers(JsonNode)에서 traceId/eventType 추출
            String traceId = getText(e.getHeaders(), "traceId");
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, e.getAggregateId(), value);
            record.headers().add(new RecordHeader("eventType", bytes(e.getType())));
            if (traceId != null) record.headers().add(new RecordHeader("traceId", bytes(traceId)));

            kafka.send(record).join();

            e.setPublished(true); // dirty checking으로 UPDATE
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
