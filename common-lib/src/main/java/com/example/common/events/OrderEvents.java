package com.example.common.events;

import lombok.*;

import java.time.Instant;
import java.util.UUID;

public class OrderEvents {
    @Getter @Builder @AllArgsConstructor @NoArgsConstructor
    public static class OrderCreated {
        private UUID orderId;
        private String userId;
        private int totalAmount;
        private Instant createdAt;
    }

    @Getter @Builder @AllArgsConstructor @NoArgsConstructor
    public static class OrderApproved {
        private UUID orderId;
        private Instant approvedAt;
    }

    @Getter @Builder @AllArgsConstructor @NoArgsConstructor
    public static class OrderCancelled {
        private UUID orderId;
        private String reason;
        private Instant cancelledAt;
    }
}