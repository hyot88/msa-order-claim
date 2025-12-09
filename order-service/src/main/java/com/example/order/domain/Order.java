package com.example.order.domain;

import jakarta.persistence.*;
import lombok.*;
import java.time.Instant;
import java.util.UUID;

/**
 * 주문 도메인 엔티티
 *
 * 시스템의 핵심 엔티티로, 주문의 전체 생명주기를 관리합니다.
 *
 * <h3>상태 전이:</h3>
 * PENDING → APPROVED (재고 확보 + 결제 완료 시)
 * PENDING → CANCELLED (재고 부족 또는 결제 실패 시)
 *
 * <h3>테이블 이름:</h3>
 * "orders"를 사용 (order는 SQL 예약어이므로 복수형 사용)
 */
@Entity @Table(name="orders")
@Getter @Setter @Builder @NoArgsConstructor @AllArgsConstructor
public class Order {
    /** 주문 고유 식별자 (UUID) */
    @Id
    private UUID id;

    /** 주문한 사용자 ID */
    private String userId;

    /** 주문 총액 (단위: 원) */
    private int totalAmount;

    /**
     * 주문 상태
     * PENDING: 주문 생성됨, 재고 확인 및 결제 대기 중
     * APPROVED: 재고 확보 및 결제 완료
     * CANCELLED: 재고 부족, 결제 실패 등의 이유로 취소됨
     */
    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    /** 주문 생성 시각 (UTC 기준) */
    private Instant createdAt;

    /** 주문 최종 수정 시각 (UTC 기준) */
    private Instant updatedAt;

    /**
     * 엔티티 저장 직전 자동 호출
     * createdAt과 updatedAt을 현재 시각으로 초기화
     */
    @PrePersist void prePersist() {
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
    }

    /**
     * 엔티티 업데이트 직전 자동 호출
     * updatedAt을 현재 시각으로 갱신
     */
    @PreUpdate void preUpdate() { this.updatedAt = Instant.now(); }
}