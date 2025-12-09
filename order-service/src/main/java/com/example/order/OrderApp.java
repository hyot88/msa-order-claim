package com.example.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * 주문 서비스 메인 애플리케이션
 *
 * <h3>주요 책임:</h3>
 * - 주문 생성, 조회, 상태 변경 (승인/취소)
 * - OrderCreated, OrderApproved, OrderCancelled 이벤트 발행
 * - Outbox 패턴을 통한 안정적인 이벤트 발행 보장
 *
 * <h3>통신 방식:</h3>
 * - IN: API Gateway를 통한 REST API 요청
 * - OUT: Kafka 이벤트 발행 (via outbox-relay)
 *
 * <h3>이벤트 수신:</h3>
 * - PaymentAuthorized: 결제 완료 시 주문 승인
 * - InventoryReservationFailed 또는 PaymentFailed: 주문 취소
 *
 * <h3>Component Scan 설정:</h3>
 * - com.example.order: 자체 컴포넌트
 * - com.example.common: OutboxEventPublisher, GlobalExceptionHandler 등 공통 컴포넌트
 *
 * <h3>JPA 설정:</h3>
 * - Repository: OutboxRepository (common-lib), OrderRepository (자체)
 * - Entity: OutboxEvent (common-lib), Order (자체)
 */
@SpringBootApplication(scanBasePackages = {"com.example.order", "com.example.common"})
@EnableJpaRepositories(basePackages = {"com.example.order", "com.example.common"})
@EntityScan(basePackages = {"com.example.order", "com.example.common"})
public class OrderApp {
    public static void main(String[] args) {
        SpringApplication.run(OrderApp.class, args);
    }
}
