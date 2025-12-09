package com.example.configserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.config.server.EnableConfigServer;

/**
 * Spring Cloud Config Server 메인 애플리케이션
 *
 * <h3>주요 역할:</h3>
 * - 중앙 집중식 설정 관리 서버 (Centralized Configuration Management)
 * - Git 저장소에서 애플리케이션 설정 파일을 읽어 제공
 * - 환경별(dev, prod 등) 설정 분리 및 동적 설정 변경 지원
 *
 * <h3>동작 방식:</h3>
 * 1. 각 마이크로서비스가 시작 시 Config Server에 설정 요청
 * 2. Config Server가 Git 저장소에서 해당 서비스의 설정 파일 조회
 * 3. 설정을 JSON/YAML 형태로 서비스에 전달
 * 4. @RefreshScope 사용 시 애플리케이션 재시작 없이 설정 갱신 가능
 *
 * <h3>설정 접근 URL 패턴:</h3>
 * - http://localhost:8888/{application}/{profile}/{label}
 * - 예: http://localhost:8888/order-service/default/master
 * - 예: http://localhost:8888/application/default
 *
 * <h3>현재 상태:</h3>
 * 이 프로젝트에서는 Config Server가 구현되어 있지만 실제로는 각 서비스가
 * 로컬 application.yml 파일을 사용하고 있습니다.
 * 향후 중앙 설정 관리로 전환할 경우 이 서버를 활성화할 수 있습니다.
 *
 * <h3>접근:</h3>
 * - Config Server: http://localhost:8888
 * - Git 저장소: application.yml에 설정된 저장소 경로 참조
 */
@SpringBootApplication
@EnableConfigServer  // Spring Cloud Config Server 활성화
public class ConfigServerApp {
    public static void main(String[] args) {
        SpringApplication.run(ConfigServerApp.class, args);
    }
}