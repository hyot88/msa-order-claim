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

@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxEventPublisher {
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final Tracer tracer;
    private final Propagator propagator;

    public void publish(String aggregateType, String aggregateId, String eventType, Object event, String eventSource) {
        try {
            log.info("Publishing outbox event: type={}, aggregateId={}", eventType, aggregateId);

            JsonNode payload = objectMapper.valueToTree(event);

            Map<String, String> headerMap = new HashMap<>();
            String traceId = Optional.ofNullable(tracer.currentSpan())
                    .map(span -> span.context().traceId())
                    .orElse(null);

            if (tracer.currentSpan() != null) {
                propagator.inject(tracer.currentSpan().context(), headerMap,
                        Map::put);
                log.debug("Injected trace context into headers: {}", headerMap);
            }

            ObjectNode headers = objectMapper.createObjectNode()
                    .put("traceId", traceId)
                    .put("eventSource", eventSource);

            for (Map.Entry<String, String> entry : headerMap.entrySet()) {
                headers.put(entry.getKey(), entry.getValue());
            }

            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .aggregateType(aggregateType)
                    .aggregateId(aggregateId)
                    .type(eventType)
                    .payload(payload)
                    .payloadHash(sha256(payload.toString()))
                    .headers(headers)
                    .published(false)
                    .build();

            outboxRepository.save(outboxEvent);
            log.info("Successfully published outbox event: type={}, aggregateId={}", eventType, aggregateId);
        } catch (Exception e) {
            log.error("Failed to publish outbox event: type={}, aggregateId={}", eventType, aggregateId, e);
            throw new OutboxPublishException("Failed to publish outbox event", e);
        }
    }

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
