package com.example.order.messaging;

import com.example.order.idempotency.ProcessedMessage;
import com.example.order.idempotency.ProcessedMessageRepository;
import com.example.order.service.OrderService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@RequiredArgsConstructor
public class PaymentEventHandler {

    private final ObjectMapper om;
    private final OrderService orderService;
    private final ProcessedMessageRepository processedRepo;

    @KafkaListener(topics = "payment.events", groupId = "order")
    public void onPaymentEvents(ConsumerRecord<String, String> rec) throws Exception {
        // 멱등 체크: (topic, partition, offset) 유니크
        try {
            processedRepo.save(ProcessedMessage.builder()
                    .topic(rec.topic())
                    .partitionId(rec.partition())
                    .offset(rec.offset())
                    .messageKey(rec.key())
                    .build());
        } catch (DataIntegrityViolationException dup) {
            // 이미 처리한 메시지 → 아무 것도 하지 않음
            return;
        }

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
