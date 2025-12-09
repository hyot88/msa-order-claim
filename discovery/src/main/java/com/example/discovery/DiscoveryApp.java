package com.example.discovery;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;

/**
 * Eureka 서비스 디스커버리 서버
 *
 * <h3>주요 역할:</h3>
 * - 모든 마이크로서비스의 인스턴스 정보를 중앙에서 관리
 * - 서비스 등록 (Service Registration)
 * - 서비스 조회 (Service Discovery)
 * - 헬스 체크를 통한 서비스 상태 모니터링
 *
 * <h3>동작 원리:</h3>
 * 1. 각 서비스가 시작 시 Eureka에 자신을 등록 (heartbeat)
 * 2. Eureka는 서비스 목록(Registry)을 유지 관리
 * 3. 클라이언트(API Gateway 등)가 서비스명으로 인스턴스 목록 조회
 * 4. 로드 밸런싱을 통해 적절한 인스턴스 선택
 *
 * <h3>서비스 등록 예시:</h3>
 * - order-service (9001)
 * - payment-service
 * - inventory-service
 * - api-gateway (8080)
 *
 * <h3>접근:</h3>
 * - Eureka 대시보드: http://localhost:8761
 * - 등록된 모든 서비스와 인스턴스를 웹 UI에서 확인 가능
 */
@SpringBootApplication
@EnableEurekaServer
public class DiscoveryApp {
    public static void main(String[] args) {
        SpringApplication.run(DiscoveryApp.class, args);
    }
}