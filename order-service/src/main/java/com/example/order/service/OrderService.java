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

/**
 * 주문 비즈니스 로직을 처리하는 서비스
 *
 * <h3>핵심 기능:</h3>
 * 1. 주문 생성: 새로운 주문을 PENDING 상태로 생성하고 OrderCreated 이벤트 발행
 * 2. 주문 승인: 재고 확보 및 결제 완료 후 APPROVED 상태로 변경
 * 3. 주문 취소: 실패 시나리오에서 CANCELLED 상태로 변경
 * 4. 주문 조회: 주문 ID로 주문 정보 조회
 *
 * <h3>Outbox 패턴 적용:</h3>
 * 모든 상태 변경 메서드는 @Transactional로 보호되며,
 * 도메인 엔티티 저장과 OutboxEvent 저장이 동일한 트랜잭션 내에서 실행됩니다.
 * 이를 통해 데이터 일관성과 이벤트 발행을 보장합니다.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {
    private final OrderRepository orderRepo;
    private final OutboxEventPublisher outboxPublisher;

    /**
     * 새로운 주문을 생성하고 OrderCreated 이벤트를 발행
     *
     * <h3>처리 흐름:</h3>
     * 1. UUID 기반 주문 ID 생성
     * 2. 주문 엔티티를 PENDING 상태로 저장
     * 3. OrderCreated 이벤트를 Outbox 테이블에 저장 (동일 트랜잭션)
     * 4. outbox-relay가 이벤트를 Kafka로 발행
     * 5. inventory-service와 payment-service가 이벤트를 수신하여 처리 시작
     *
     * @param userId 주문한 사용자 ID
     * @param totalAmount 주문 총액
     * @return 생성된 주문의 UUID
     */
    @Transactional
    public UUID create(String userId, int totalAmount) {
        UUID orderId = UUID.randomUUID();
        log.info("[order] Creating order with ID: {}", orderId);

        // 1. 주문 엔티티 저장
        Order order = Order.builder()
                .id(orderId)
                .userId(userId)
                .totalAmount(totalAmount)
                .status(OrderStatus.PENDING)
                .build();
        orderRepo.save(order);

        // 2. OrderCreated 이벤트 발행 (Outbox 패턴)
        OrderEvents.OrderCreated event = OrderEvents.OrderCreated.builder()
                .orderId(orderId)
                .userId(userId)
                .totalAmount(totalAmount)
                .createdAt(Instant.now())
                .build();

        outboxPublisher.publish("ORDER", orderId.toString(), "OrderCreated", event, "order-service");

        return orderId;
    }

    /**
     * 주문을 승인 상태로 변경하고 OrderApproved 이벤트를 발행
     *
     * <h3>호출 시점:</h3>
     * PaymentEventHandler가 PaymentAuthorized 이벤트를 수신했을 때 호출됩니다.
     * 재고 예약과 결제가 모두 성공했음을 의미합니다.
     *
     * @param orderId 승인할 주문 ID
     * @throws OrderNotFoundException 주문이 존재하지 않을 경우
     */
    @Transactional
    public void approve(UUID orderId) {
        log.info("[order] Approving order with ID: {}", orderId);

        // 주문 조회 및 상태 변경
        Order order = orderRepo.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));
        order.setStatus(OrderStatus.APPROVED);
        orderRepo.save(order);

        // OrderApproved 이벤트 발행
        OrderEvents.OrderApproved event = OrderEvents.OrderApproved.builder()
                .orderId(orderId)
                .approvedAt(Instant.now())
                .build();

        outboxPublisher.publish("ORDER", orderId.toString(), "OrderApproved", event, "order-service");
    }

    /**
     * 주문을 취소 상태로 변경하고 OrderCancelled 이벤트를 발행
     *
     * <h3>호출 시점:</h3>
     * - 재고 부족으로 InventoryReservationFailed 이벤트 수신 시
     * - 결제 실패로 PaymentFailed 이벤트 수신 시
     * - 사용자가 명시적으로 주문 취소 API를 호출했을 때
     *
     * <h3>보상 트랜잭션 (Saga Compensation):</h3>
     * OrderCancelled 이벤트를 발행하면 다른 서비스들이 보상 작업을 수행합니다:
     * - inventory-service: 예약된 재고 해제
     * - payment-service: 결제 취소 (환불)
     *
     * @param orderId 취소할 주문 ID
     * @param reason 취소 사유 (예: "재고 부족", "결제 실패")
     * @throws OrderNotFoundException 주문이 존재하지 않을 경우
     */
    @Transactional
    public void cancel(UUID orderId, String reason) {
        log.info("[order] Cancelling order with ID: {}, reason: {}", orderId, reason);

        // 주문 조회 및 상태 변경
        Order order = orderRepo.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));
        order.setStatus(OrderStatus.CANCELLED);
        orderRepo.save(order);

        // OrderCancelled 이벤트 발행
        OrderEvents.OrderCancelled event = OrderEvents.OrderCancelled.builder()
                .orderId(orderId)
                .reason(reason)
                .cancelledAt(Instant.now())
                .build();

        outboxPublisher.publish("ORDER", orderId.toString(), "OrderCancelled", event, "order-service");
    }

    /**
     * 주문 ID로 주문 정보를 조회
     *
     * @param orderId 조회할 주문 ID
     * @return 주문 엔티티
     * @throws OrderNotFoundException 주문이 존재하지 않을 경우
     */
    public Order getOrder(UUID orderId) {
        log.info("[order] Retrieving order with ID: {}", orderId);
        return orderRepo.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));
    }
}
