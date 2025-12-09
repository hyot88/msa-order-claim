package com.example.order.api;

import com.example.order.domain.Order;
import com.example.order.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
@RequestMapping("/orders")
public class OrderController {
    private final OrderService orderService;

    @PostMapping
    public ResponseEntity<Map<String, UUID>> create(@Valid @RequestBody CreateOrderRequest req) {
        UUID id = orderService.create(req.userId(), req.totalAmount());
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(Map.of("orderId", id));
    }

    @GetMapping("/{id}")
    public ResponseEntity<OrderResponse> get(@PathVariable UUID id) {
        Order order = orderService.getOrder(id);
        OrderResponse response = OrderResponse.builder()
                .id(order.getId())
                .userId(order.getUserId())
                .totalAmount(order.getTotalAmount())
                .status(order.getStatus())
                .build();
        return ResponseEntity.ok(response);
    }
}