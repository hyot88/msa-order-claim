package com.example.order.service;

import com.example.common.events.OrderEvents;
import com.example.common.exception.OrderNotFoundException;
import com.example.common.outbox.OutboxEventPublisher;
import com.example.order.domain.Order;
import com.example.order.domain.OrderStatus;
import com.example.order.repo.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {
    private final OrderRepository orderRepo;
    private final OutboxEventPublisher outboxPublisher;

    @Transactional
    public UUID create(String userId, int totalAmount) {
        UUID orderId = UUID.randomUUID();
        log.info("[order] Creating order with ID: {}", orderId);

        Order order = Order.builder()
                .id(orderId)
                .userId(userId)
                .totalAmount(totalAmount)
                .status(OrderStatus.PENDING)
                .build();
        orderRepo.save(order);

        OrderEvents.OrderCreated event = OrderEvents.OrderCreated.builder()
                .orderId(orderId)
                .userId(userId)
                .totalAmount(totalAmount)
                .createdAt(Instant.now())
                .build();

        outboxPublisher.publish("ORDER", orderId.toString(), "OrderCreated", event, "order-service");

        return orderId;
    }

    @Transactional
    public void approve(UUID orderId) {
        log.info("[order] Approving order with ID: {}", orderId);
        Order order = orderRepo.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));
        order.setStatus(OrderStatus.APPROVED);
        orderRepo.save(order);

        OrderEvents.OrderApproved event = OrderEvents.OrderApproved.builder()
                .orderId(orderId)
                .approvedAt(Instant.now())
                .build();

        outboxPublisher.publish("ORDER", orderId.toString(), "OrderApproved", event, "order-service");
    }

    @Transactional
    public void cancel(UUID orderId, String reason) {
        log.info("[order] Cancelling order with ID: {}, reason: {}", orderId, reason);
        Order order = orderRepo.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));
        order.setStatus(OrderStatus.CANCELLED);
        orderRepo.save(order);

        OrderEvents.OrderCancelled event = OrderEvents.OrderCancelled.builder()
                .orderId(orderId)
                .reason(reason)
                .cancelledAt(Instant.now())
                .build();

        outboxPublisher.publish("ORDER", orderId.toString(), "OrderCancelled", event, "order-service");
    }

    public Order getOrder(UUID orderId) {
        log.info("[order] Retrieving order with ID: {}", orderId);
        return orderRepo.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));
    }
}
