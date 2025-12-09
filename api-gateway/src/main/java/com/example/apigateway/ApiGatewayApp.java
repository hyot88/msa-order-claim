package com.example.apigateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * API Gateway 메인 애플리케이션
 *
 * <h3>주요 역할:</h3>
 * - 모든 클라이언트 요청의 단일 진입점 (Single Entry Point)
 * - 요청을 적절한 마이크로서비스로 라우팅
 * - Circuit Breaker를 통한 장애 격리 및 폴백 처리
 * - 로드 밸런싱 (Eureka를 통한 서비스 인스턴스 선택)
 *
 * <h3>라우팅 규칙:</h3>
 * - /orders/** → order-service (포트 9001)
 * - /claims/** → claim-service
 *
 * <h3>Circuit Breaker 패턴:</h3>
 * order-service에 Circuit Breaker가 적용되어 있습니다.
 * - 정상: 모든 요청 전달
 * - 실패율 50% 초과: Circuit OPEN → fallback 응답 반환
 * - 10초 후: Circuit HALF_OPEN → 일부 요청만 테스트
 *
 * <h3>서비스 디스커버리 통합:</h3>
 * Eureka Client로 등록되어 discovery 서버에서 서비스 목록을 조회하고,
 * lb:// 프로토콜로 로드 밸런싱을 수행합니다.
 *
 * <h3>접근 방식:</h3>
 * 외부 클라이언트 → Gateway (localhost:8080) → 마이크로서비스들
 */
@SpringBootApplication
public class ApiGatewayApp {
    public static void main(String[] args) {
        SpringApplication.run(ApiGatewayApp.class, args);
    }
}
