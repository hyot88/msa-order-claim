package com.example.inventory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaHandlers {

    private final ObjectMapper om;
    private final KafkaTemplate<String, String> kafka;

    @Value("${inventory.failRate:0.0}")
    double failRate;

    @KafkaListener(topics = "order.events") // 👈 containerFactory 지정 불필요(기본 bean 사용)
    public void onOrderEvents(ConsumerRecord<String, String> rec) throws Exception {
        ProducerRecord<String, String> out;
        Header h = rec.headers().lastHeader("traceId");
        String traceId = new String(h.value(), StandardCharsets.UTF_8);

        log.info("[inventory] consume topic={} key={} value={}", rec.topic(), rec.key(), rec.value());

        JsonNode node = om.readTree(rec.value());
        // 우리가 발행한 OrderCreated 포맷인지 확인
        if (!node.has("orderId") || !node.has("userId") || !node.has("totalAmount")) {
            log.debug("[inventory] ignore non-OrderCreated payload");
            return;
        }

        UUID orderId = UUID.fromString(node.get("orderId").asText());
        // 재고 관리 시스템에서 재고 부족 상황을 인위적으로 시뮬레이션하기 위해 사용
        boolean fail = ThreadLocalRandom.current().nextDouble() < failRate;

        Map<String, Object> evt = fail
                ? Map.of("orderId", orderId.toString(), "reason", "OUT_OF_STOCK", "failedAt", Instant.now().toString())
                : Map.of("orderId", orderId.toString(), "reservedAt", Instant.now().toString());

        String payload = om.writeValueAsString(evt);

        out = new ProducerRecord<>("inventory.events", orderId.toString(), payload);
        out.headers().add(new RecordHeader("traceId", traceId.getBytes(StandardCharsets.UTF_8)));
        kafka.send(out).whenComplete((md, ex) -> {
            if (ex != null) {
                log.error("[inventory] publish inventory.events FAILED orderId={}", orderId, ex);
            } else {
                log.info("[inventory] publish inventory.events OK partition={} offset={}",
                        md.getRecordMetadata().partition(), md.getRecordMetadata().offset());
            }
        });
    }
}
