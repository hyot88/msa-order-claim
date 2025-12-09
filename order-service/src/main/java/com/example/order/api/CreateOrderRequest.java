package com.example.order.api;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record CreateOrderRequest(
        @NotBlank(message = "User ID must not be blank")
        @Size(min = 1, max = 100, message = "User ID must be between 1 and 100 characters")
        String userId,

        @Min(value = 1, message = "Total amount must be at least 1")
        @Max(value = 1000000000, message = "Total amount must not exceed 1 billion")
        int totalAmount
) {
}