package com.example.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * 결제 서비스 메인 애플리케이션
 *
 * <h3>주요 책임:</h3>
 * - 재고 예약 완료 이벤트(InventoryReserved) 수신
 * - 결제 처리 (성공/실패 시뮬레이션)
 * - PaymentAuthorized 또는 PaymentFailed 이벤트 발행
 *
 * <h3>Saga 패턴에서의 역할:</h3>
 * OrderCreated → inventory-service → payment-service ★ → order-service
 *
 * 이 서비스는 주문 Saga의 두 번째 단계를 담당합니다.
 * 재고가 성공적으로 예약되면 결제를 처리하고, 그 결과를
 * order-service에 전달하여 주문 최종 승인/취소를 결정하게 합니다.
 *
 * <h3>통신 방식:</h3>
 * - IN: inventory.events 토픽 구독 (InventoryReserved 수신)
 * - OUT: payment.events 토픽으로 이벤트 발행
 *
 * <h3>특징:</h3>
 * - JPA 미사용 (상태를 DB에 저장하지 않음)
 * - 이벤트 기반 Stateless 서비스
 * - 실패 시뮬레이션 기능 포함 (application.yml의 payment-fail-rate)
 */
@EnableKafka
@SpringBootApplication
public class PaymentApp {
    public static void main(String[] args) { SpringApplication.run(PaymentApp.class, args); }
}
