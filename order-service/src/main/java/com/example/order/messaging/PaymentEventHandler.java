package com.example.order.messaging;

import com.example.order.service.OrderService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@RequiredArgsConstructor
public class PaymentEventHandler {

    private final ObjectMapper om;
    private final OrderService orderService;

    @KafkaListener(topics = "payment.events", groupId = "order")
    public void onPaymentEvents(ConsumerRecord<String, String> rec) throws Exception {
        JsonNode node = om.readTree(rec.value());
        if (!node.has("orderId")) return;
        UUID orderId = UUID.fromString(node.get("orderId").asText());

        boolean isFailure = node.has("reason");
        if (isFailure) {
            orderService.cancel(orderId, node.get("reason").asText());
        } else {
            orderService.approve(orderId);
        }
    }
}
