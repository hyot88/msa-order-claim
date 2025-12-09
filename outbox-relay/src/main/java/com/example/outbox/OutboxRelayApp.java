package com.example.outbox;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Outbox Relay 서비스 메인 애플리케이션
 *
 * <h3>핵심 역할:</h3>
 * Outbox 패턴의 핵심 컴포넌트로, 데이터베이스에 저장된 미발행 이벤트를
 * 주기적으로 조회하여 Kafka로 발행합니다.
 *
 * <h3>동작 원리:</h3>
 * 1. OutboxRelay가 @Scheduled로 500ms마다 outbox_event 테이블 폴링
 * 2. published=false인 이벤트를 FOR UPDATE SKIP LOCKED로 조회
 * 3. 각 이벤트를 적절한 Kafka 토픽으로 발행
 * 4. published=true로 업데이트하여 중복 발행 방지
 *
 * <h3>왜 필요한가?</h3>
 * - 서비스가 이벤트 저장 후 Kafka 발행 전에 장애가 발생해도 이벤트는 유실되지 않음
 * - 트랜잭션 범위에서 도메인 엔티티와 이벤트를 원자적으로 저장
 * - Relay가 재시작되어도 미발행 이벤트를 계속 발행
 *
 * <h3>다중 인스턴스 지원:</h3>
 * FOR UPDATE SKIP LOCKED 쿼리로 여러 relay 인스턴스가 동시에 실행되어도
 * 각 이벤트가 정확히 한 번만 처리됩니다.
 *
 * <h3>Component Scan 설정:</h3>
 * - com.example.outbox: OutboxRelay 컴포넌트
 * - com.example.common: OutboxEvent, OutboxRepository (common-lib)
 */
@SpringBootApplication(scanBasePackages = {"com.example.outbox", "com.example.common"})
@EnableJpaRepositories(basePackages = {"com.example.outbox", "com.example.common"})
@EntityScan(basePackages = {"com.example.outbox", "com.example.common"})
public class OutboxRelayApp {
    public static void main(String[] args) {
        SpringApplication.run(OutboxRelayApp.class, args);
    }
}