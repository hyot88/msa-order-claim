package com.example.order.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

@Entity @Table(name="outbox_event",
        uniqueConstraints = @UniqueConstraint(name="uk_outbox_once", columnNames = {"aggregate_id", "type", "payload_hash"}))
@Getter @Setter @Builder @NoArgsConstructor @AllArgsConstructor
public class OutboxEvent {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String aggregateType;   // e.g., "ORDER"
    private String aggregateId;     // UUID string
    private String type;            // e.g., "OrderCreated"

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private JsonNode payload;       // 또는 ObjectNode, Map<String,Object>
    private String payload_hash;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private JsonNode headers;       // 또는 ObjectNode, Map<String,Object>

    private boolean published;
    private Instant createdAt;

    @PrePersist void prePersist() { this.createdAt = Instant.now(); }
}