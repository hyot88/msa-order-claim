package com.example.payment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
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

@Component
@RequiredArgsConstructor
public class KafkaHandlers {
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper om;

    @Value("${payment.failRate:0.0}")
    double failRate;

    @KafkaListener(topics = "inventory.events", groupId = "payment")
    public void onInventoryEvents(ConsumerRecord<String, String> rec) throws Exception {
        ProducerRecord<String, String> out;
        Header h = rec.headers().lastHeader("traceId");
        String traceId = new String(h.value(), StandardCharsets.UTF_8);

        JsonNode node = om.readTree(rec.value());
        if (!node.has("orderId")) return;

        UUID orderId = UUID.fromString(node.get("orderId").asText());

        boolean isFailure = node.has("reason"); // 간단 구분: reason 있으면 실패
        if (isFailure) {
            // 보상 플로우: 결제 시도 안하고 실패 이벤트 전달(또는 PaymentCancelled)
            var evt = Map.of("orderId", orderId.toString(), "failedAt", Instant.now().toString(), "reason", "INVENTORY_FAIL");
            out = new ProducerRecord<>("payment.events", orderId.toString(), om.writeValueAsString(evt));
            out.headers().add(new RecordHeader("traceId", traceId.getBytes(StandardCharsets.UTF_8)));
            kafka.send(out);
            return;
        }

        // 결제 관리 시스템에서 결제 실패 상황을 인위적으로 시뮬레이션하기 위해 사용
        boolean fail = ThreadLocalRandom.current().nextDouble() < failRate;
        if (!fail) {
            var evt = Map.of("orderId", orderId.toString(), "authorizedAt", Instant.now().toString());
            out = new ProducerRecord<>("payment.events", orderId.toString(), om.writeValueAsString(evt));
            out.headers().add(new RecordHeader("traceId", traceId.getBytes(StandardCharsets.UTF_8)));
            kafka.send(out);
        } else {
            var evt = Map.of("orderId", orderId.toString(), "failedAt", Instant.now().toString(), "reason", "PAYMENT_DECLINED");
            out = new ProducerRecord<>("payment.events", orderId.toString(), om.writeValueAsString(evt));
            out.headers().add(new RecordHeader("traceId", traceId.getBytes(StandardCharsets.UTF_8)));
            kafka.send(out);
        }
    }
}
