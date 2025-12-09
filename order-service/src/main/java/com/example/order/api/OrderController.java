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

/**
 * 주문 REST API 컨트롤러
 *
 * <h3>API 엔드포인트:</h3>
 * - POST /orders: 새로운 주문 생성
 * - GET /orders/{id}: 주문 조회
 *
 * <h3>접근 방식:</h3>
 * 이 API는 API Gateway를 통해 노출됩니다.
 * 클라이언트 → API Gateway (localhost:8080) → order-service (localhost:9001)
 *
 * <h3>에러 처리:</h3>
 * GlobalExceptionHandler가 모든 예외를 포착하여 일관된 에러 응답을 반환합니다.
 */
@RestController
@RequiredArgsConstructor
@RequestMapping("/orders")
public class OrderController {
    private final OrderService orderService;

    /**
     * 새로운 주문 생성
     *
     * <h3>요청 예시:</h3>
     * <pre>
     * POST /orders
     * Content-Type: application/json
     * {
     *   "userId": "hyot.ahn",
     *   "totalAmount": 17500
     * }
     * </pre>
     *
     * <h3>응답 예시:</h3>
     * <pre>
     * HTTP 201 Created
     * {
     *   "orderId": "39732f2c-713b-4deb-a012-f45b64d6e4f8"
     * }
     * </pre>
     *
     * @param req 주문 생성 요청 (userId, totalAmount)
     * @return 생성된 주문의 ID
     */
    @PostMapping
    public ResponseEntity<Map<String, UUID>> create(@Valid @RequestBody CreateOrderRequest req) {
        UUID id = orderService.create(req.userId(), req.totalAmount());
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(Map.of("orderId", id));
    }

    /**
     * 주문 정보 조회
     *
     * @param id 조회할 주문 ID
     * @return 주문 정보 (id, userId, totalAmount, status)
     */
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