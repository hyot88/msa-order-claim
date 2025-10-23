package com.example.order.api;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record CreateOrderRequest(
        @NotBlank String userId,
        @Min(1) int totalAmount
) {}