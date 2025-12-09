package com.example.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * 재고 서비스 메인 애플리케이션
 *
 * <h3>주요 책임:</h3>
 * - 주문 생성 이벤트(OrderCreated) 수신
 * - 재고 확인 및 예약 처리 (성공/실패 시뮬레이션)
 * - InventoryReserved 또는 InventoryReservationFailed 이벤트 발행
 *
 * <h3>Saga 패턴에서의 역할:</h3>
 * OrderCreated → inventory-service ★ → payment-service → order-service
 *
 * 이 서비스는 주문 Saga의 첫 번째 단계를 담당합니다.
 * 새로운 주문이 생성되면 재고를 확인하고, 재고가 충분하면
 * 예약 처리 후 payment-service로 결제 요청을 전달합니다.
 *
 * <h3>통신 방식:</h3>
 * - IN: order.events 토픽 구독 (OrderCreated 수신)
 * - OUT: inventory.events 토픽으로 이벤트 발행
 *
 * <h3>특징:</h3>
 * - JPA 미사용 (상태를 DB에 저장하지 않음)
 * - 이벤트 기반 Stateless 서비스
 * - 재고 부족 시뮬레이션 기능 포함 (application.yml의 inventory-fail-rate)
 */
@EnableKafka
@SpringBootApplication
public class InventoryApp {
    public static void main(String[] args) { SpringApplication.run(InventoryApp.class, args); }
}
