package com.example.order.idempotency;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;

@Entity
@Table(name = "processed_message",
        uniqueConstraints = @UniqueConstraint(name="uk_topic_part_offset", columnNames = {"topic","partition","offset"}))
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class ProcessedMessage {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String topic;
    @Column(name = "partition")
    private int partitionId;
    @Column(name = "offset_value")
    private long offset;
    @Column(name="message_key")
    private String messageKey;

    private Instant processedAt;
    @PrePersist void pp(){ if(processedAt==null) processedAt=Instant.now(); }
}
