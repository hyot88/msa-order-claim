package com.example.payment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

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
        JsonNode node = om.readTree(rec.value());
        if (!node.has("orderId")) return;

        UUID orderId = UUID.fromString(node.get("orderId").asText());

        boolean isFailure = node.has("reason"); // 간단 구분: reason 있으면 실패
        if (isFailure) {
            // 보상 플로우: 결제 시도 안하고 실패 이벤트 전달(또는 PaymentCancelled)
            var evt = Map.of("orderId", orderId.toString(), "failedAt", Instant.now().toString(), "reason", "INVENTORY_FAIL");
            kafka.send("payment.events", orderId.toString(), om.writeValueAsString(evt));
            return;
        }

        boolean fail = ThreadLocalRandom.current().nextDouble() < failRate;
        if (!fail) {
            var evt = Map.of("orderId", orderId.toString(), "authorizedAt", Instant.now().toString());
            kafka.send("payment.events", orderId.toString(), om.writeValueAsString(evt));
        } else {
            var evt = Map.of("orderId", orderId.toString(), "failedAt", Instant.now().toString(), "reason", "PAYMENT_DECLINED");
            kafka.send("payment.events", orderId.toString(), om.writeValueAsString(evt));
        }
    }
}
