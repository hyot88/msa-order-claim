package com.example.order.service;

import com.example.common.events.OrderEvents;
import com.example.order.domain.Order;
import com.example.order.domain.OrderStatus;
import com.example.order.outbox.OutboxEvent;
import com.example.order.outbox.OutboxRepository;
import com.example.order.repo.OrderRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.tracing.Tracer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderService {
    private final OrderRepository orderRepo;
    private final OutboxRepository outboxRepo;
    private final ObjectMapper om;
    private final Tracer tracer;

    @Transactional
    public UUID create(String userId, int totalAmount) {
        UUID orderId = UUID.randomUUID();

        Order order = Order.builder()
                .id(orderId)
                .userId(userId)
                .totalAmount(totalAmount)
                .status(OrderStatus.PENDING)
                .build();
        orderRepo.save(order);

        OrderEvents.OrderCreated evt = OrderEvents.OrderCreated.builder()
                .orderId(orderId)
                .userId(userId)
                .totalAmount(totalAmount)
                .createdAt(Instant.now())
                .build();

        // payload/headers를 JSON 문자열로 저장
        try {
            String traceId = Optional.ofNullable(tracer.currentSpan())
                    .map(span -> span.context().traceId())
                    .orElse(null);

            JsonNode payload = om.valueToTree(evt); // evt -> JsonNode
            ObjectNode headers = om.createObjectNode()
                    .put("traceId", traceId)
                    .put("eventSource", "order-service");
            outboxRepo.save(OutboxEvent.builder()
                    .aggregateType("ORDER")
                    .aggregateId(orderId.toString())
                    .type("OrderCreated")
                    .payload(payload)
                    .headers(headers)
                    .published(false)
                    .build());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return orderId;
    }

    @Transactional
    public void approve(UUID orderId) {
        Order order = orderRepo.findById(orderId).orElseThrow();
        order.setStatus(OrderStatus.APPROVED);
        orderRepo.save(order);

        // 승인 이벤트도 Outbox에 기록 (선언적 일관성 유지)
        try {
            var evt = OrderEvents.OrderApproved.builder()
                    .orderId(orderId).approvedAt(Instant.now()).build();
            JsonNode payload = om.valueToTree(evt); // evt -> JsonNode
            ObjectNode headers = om.createObjectNode()
                    .put("eventSource", "order-service");

            outboxRepo.save(OutboxEvent.builder()
                    .aggregateType("ORDER")
                    .aggregateId(orderId.toString())
                    .type("OrderApproved")
                    .payload(payload)
                    .headers(headers)
                    .published(false)
                    .build());
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    @Transactional
    public void cancel(UUID orderId, String reason) {
        Order order = orderRepo.findById(orderId).orElseThrow();
        order.setStatus(OrderStatus.CANCELLED);
        orderRepo.save(order);
        try {
            var evt = OrderEvents.OrderCancelled.builder()
                    .orderId(orderId).reason(reason).cancelledAt(Instant.now()).build();
            JsonNode payload = om.valueToTree(evt); // evt -> JsonNode
            ObjectNode headers = om.createObjectNode()
                    .put("eventSource", "order-service");
            outboxRepo.save(OutboxEvent.builder()
                    .aggregateType("ORDER")
                    .aggregateId(orderId.toString())
                    .type("OrderCancelled")
                    .payload(payload)
                    .headers(headers)
                    .published(false)
                    .build());
        } catch (Exception e) { throw new RuntimeException(e); }
    }
}