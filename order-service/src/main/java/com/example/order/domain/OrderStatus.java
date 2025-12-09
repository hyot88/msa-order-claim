package com.example.order.domain;

/**
 * 주문 상태 열거형
 *
 * PENDING: 주문이 생성되었으나 아직 처리 중 (재고 확인 및 결제 대기)
 * APPROVED: 재고 예약 및 결제가 완료되어 주문이 확정됨
 * CANCELLED: 재고 부족, 결제 실패 등의 이유로 주문이 취소됨
 *
 * 상태 전이:
 * - PENDING → APPROVED: payment-service가 PaymentAuthorized 이벤트 발행 시
 * - PENDING → CANCELLED: 재고 부족 또는 결제 실패 시
 */
public enum OrderStatus { PENDING, APPROVED, CANCELLED }
