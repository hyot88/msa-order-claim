package com.example.common.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

/**
 * Outbox 패턴을 구현하기 위한 이벤트 엔티티
 *
 * <h3>Outbox 패턴이란?</h3>
 * 마이크로서비스에서 데이터베이스 트랜잭션과 메시지 발행을 원자적으로 처리하기 위한 패턴입니다.
 *
 * <h3>동작 방식:</h3>
 * 1. 비즈니스 로직 수행 시 도메인 엔티티와 OutboxEvent를 동일한 트랜잭션 내에서 저장
 * 2. outbox-relay 서비스가 주기적으로 미발행(published=false) 이벤트를 조회
 * 3. 이벤트를 Kafka로 발행한 후 published=true로 업데이트
 *
 * <h3>장점:</h3>
 * - At-least-once 전달 보장 (메시지 유실 방지)
 * - 분산 트랜잭션 없이 데이터 일관성 유지
 * - 서비스 장애 시에도 이벤트 발행 보장
 *
 * <h3>중복 방지:</h3>
 * uniqueConstraint(aggregate_id, type, payload_hash)로 동일한 이벤트의 중복 저장을 방지합니다.
 */
@Entity
@Table(name = "outbox_event",
        uniqueConstraints = @UniqueConstraint(name = "uk_outbox_once",
                columnNames = {"aggregate_id", "type", "payload_hash"}))
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEvent {

    /** 이벤트 고유 식별자 (자동 증가) */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** 애그리게이트 타입 (예: "Order", "Payment") */
    @Column(name = "aggregate_type")
    private String aggregateType;

    /** 애그리게이트 식별자 (예: 주문 ID, 결제 ID) */
    @Column(name = "aggregate_id")
    private String aggregateId;

    /** 이벤트 타입 (예: "OrderCreated", "PaymentCompleted") */
    @Column(name = "type")
    private String type;

    /**
     * 이벤트 페이로드 (JSON 형식)
     * PostgreSQL의 JSONB 타입으로 저장되어 효율적인 쿼리 및 인덱싱 지원
     */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private JsonNode payload;

    /**
     * 페이로드의 SHA-256 해시값
     * uniqueConstraint의 일부로 사용되어 동일한 이벤트의 중복 저장 방지
     */
    @Column(name = "payload_hash")
    private String payloadHash;

    /**
     * 이벤트 메타데이터 헤더 (JSON 형식)
     * traceId, eventSource 등 분산 추적 및 디버깅에 필요한 정보 포함
     */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private JsonNode headers;

    /**
     * 발행 완료 여부
     * false: 아직 Kafka로 발행되지 않음 (outbox-relay가 처리할 대상)
     * true: 이미 Kafka로 발행 완료
     */
    @Column(name = "published")
    private boolean published;

    /** 이벤트 생성 시각 (UTC 기준) */
    @Column(name = "created_at")
    private Instant createdAt;

    /**
     * 엔티티가 영속화되기 전에 자동으로 생성 시각 설정
     */
    @PrePersist
    void prePersist() {
        this.createdAt = Instant.now();
    }
}
