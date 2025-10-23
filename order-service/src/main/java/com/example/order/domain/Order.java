package com.example.order.domain;

import jakarta.persistence.*;
import lombok.*;
import java.time.Instant;
import java.util.UUID;

@Entity @Table(name="orders")
@Getter @Setter @Builder @NoArgsConstructor @AllArgsConstructor
public class Order {
    @Id
    private UUID id;
    private String userId;
    private int totalAmount;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    private Instant createdAt;
    private Instant updatedAt;

    @PrePersist void prePersist() {
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
    }
    @PreUpdate void preUpdate() { this.updatedAt = Instant.now(); }
}