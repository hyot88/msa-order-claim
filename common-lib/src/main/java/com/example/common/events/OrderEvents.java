package com.example.common.events;

import lombok.*;

import java.time.Instant;
import java.util.UUID;

/**
 * 주문 도메인 이벤트를 정의하는 클래스
 *
 * 마이크로서비스 간 이벤트 기반 통신을 위한 메시지 스키마를 제공합니다.
 * 모든 이벤트는 Kafka 토픽을 통해 전송되며, 다른 서비스들이 구독하여 처리합니다.
 *
 * 이벤트 흐름:
 * 1. OrderCreated: 주문이 생성되면 발행 (order-service -> inventory-service, payment-service)
 * 2. OrderApproved: 재고 확보 및 결제 완료 후 발행 (order-service)
 * 3. OrderCancelled: 주문 취소 시 발행 (order-service)
 */
public class OrderEvents {

    /**
     * 주문 생성 이벤트
     *
     * 새로운 주문이 시스템에 등록되었을 때 발행됩니다.
     * 이 이벤트를 받은 inventory-service는 재고를 확인/예약하고,
     * payment-service는 결제 처리를 시작합니다.
     */
    @Getter @Builder @AllArgsConstructor @NoArgsConstructor
    public static class OrderCreated {
        /** 주문 고유 식별자 (UUID) */
        private UUID orderId;

        /** 주문한 사용자 ID */
        private String userId;

        /** 주문 총액 (단위: 원) */
        private int totalAmount;

        /** 주문 생성 시각 (UTC 기준) */
        private Instant createdAt;
    }

    /**
     * 주문 승인 이벤트
     *
     * 재고 예약과 결제가 모두 성공적으로 완료되어 주문이 승인되었을 때 발행됩니다.
     * 주문 상태가 CREATED에서 APPROVED로 변경됩니다.
     */
    @Getter @Builder @AllArgsConstructor @NoArgsConstructor
    public static class OrderApproved {
        /** 승인된 주문의 ID */
        private UUID orderId;

        /** 주문 승인 시각 (UTC 기준) */
        private Instant approvedAt;
    }

    /**
     * 주문 취소 이벤트
     *
     * 주문이 취소되었을 때 발행됩니다.
     * 재고 예약 해제, 결제 취소 등의 보상 트랜잭션(Saga Compensation)을 트리거합니다.
     */
    @Getter @Builder @AllArgsConstructor @NoArgsConstructor
    public static class OrderCancelled {
        /** 취소된 주문의 ID */
        private UUID orderId;

        /** 취소 사유 (예: "재고 부족", "결제 실패") */
        private String reason;

        /** 주문 취소 시각 (UTC 기준) */
        private Instant cancelledAt;
    }
}