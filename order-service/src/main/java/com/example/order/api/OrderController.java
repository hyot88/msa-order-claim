package com.example.order.api;

import com.example.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequiredArgsConstructor
@RequestMapping("/orders")
public class OrderController {
    private final OrderService orderService;

    @PostMapping
    public ResponseEntity<?> create(@RequestBody CreateOrderRequest req) {
        UUID id = orderService.create(req.userId(), req.totalAmount());
        return ResponseEntity.ok().body(id);
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> get(@PathVariable UUID id) {
        // 필요시 조회용 서비스/DTO 추가. 임시로 id만 반환
        return ResponseEntity.ok(id);
    }
}