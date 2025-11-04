package com.example.order.service;

import com.example.common.events.OrderEvents;
import com.example.order.domain.Order;
import com.example.order.domain.OrderStatus;
import com.example.order.outbox.OutboxEvent;
import com.example.order.outbox.OutboxRepository;
import com.example.order.repo.OrderRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {
    private final OrderRepository orderRepo;
    private final OutboxRepository outboxRepo;
    private final ObjectMapper om;
    private final Tracer tracer;
    private final Propagator propagator;

    @Transactional
    public UUID create(String userId, int totalAmount) {
        UUID orderId = UUID.randomUUID();

        Order order = Order.builder()
                .id(orderId)
                .userId(userId)
                .totalAmount(totalAmount)
                .status(OrderStatus.PENDING)
                .build();
        orderRepo.save(order);

        OrderEvents.OrderCreated evt = OrderEvents.OrderCreated.builder()
                .orderId(orderId)
                .userId(userId)
                .totalAmount(totalAmount)
                .createdAt(Instant.now())
                .build();

        // payload/headers를 JSON 문자열로 저장
        try {
            log.info("Creating order event with ID: {}", orderId);

            JsonNode payload = om.valueToTree(evt); // evt -> JsonNode

            // 현재 스팬의 컨텍스트를 헤더에 주입
            Map<String, String> headerMap = new HashMap<>();

            // 기존 방식의 traceId 헤더도 호환성을 위해 유지
            String traceId = Optional.ofNullable(tracer.currentSpan())
                    .map(span -> span.context().traceId())
                    .orElse(null);

            if (tracer.currentSpan() != null) {
                propagator.inject(tracer.currentSpan().context(), headerMap, 
                        (carrier, key, value) -> carrier.put(key, value));
                log.debug("Injected trace context into headers: {}", headerMap);
            }

            // 주입된 헤더를 JSON으로 변환
            ObjectNode headers = om.createObjectNode()
                    .put("traceId", traceId)
                    .put("eventSource", "order-service");

            // 트레이싱 헤더 추가
            for (Map.Entry<String, String> entry : headerMap.entrySet()) {
                headers.put(entry.getKey(), entry.getValue());
            }
            outboxRepo.save(OutboxEvent.builder()
                    .aggregateType("ORDER")
                    .aggregateId(orderId.toString())
                    .type("OrderCreated")
                    .payload(payload)
                    .payload_hash(sha256(payload.asText())) // 논리적 중복 이벤트 인서트를 DB 레벨에서 한번 더 차단
                    .headers(headers)
                    .published(false)
                    .build());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return orderId;
    }

    private String sha256(String s) {
        try {
            var md = java.security.MessageDigest.getInstance("SHA-256");
            byte[] d = md.digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : d) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    @Transactional
    public void approve(UUID orderId) {
        Order order = orderRepo.findById(orderId).orElseThrow();
        order.setStatus(OrderStatus.APPROVED);
        orderRepo.save(order);

        // 승인 이벤트도 Outbox에 기록 (선언적 일관성 유지)
//        try {
//            log.info("Creating order approval event with ID: {}", orderId);
//
//            var evt = OrderEvents.OrderApproved.builder()
//                    .orderId(orderId).approvedAt(Instant.now()).build();
//            JsonNode payload = om.valueToTree(evt); // evt -> JsonNode
//
//            // 현재 스팬의 컨텍스트를 헤더에 주입
//            Map<String, String> headerMap = new HashMap<>();
//
//            // 기존 방식의 traceId 헤더도 호환성을 위해 유지
//            String traceId = Optional.ofNullable(tracer.currentSpan())
//                    .map(span -> span.context().traceId())
//                    .orElse(null);
//
//            if (tracer.currentSpan() != null) {
//                propagator.inject(tracer.currentSpan().context(), headerMap,
//                        (carrier, key, value) -> carrier.put(key, value));
//                log.debug("Injected trace context into headers: {}", headerMap);
//            }
//
//            // 주입된 헤더를 JSON으로 변환
//            ObjectNode headers = om.createObjectNode()
//                    .put("traceId", traceId)
//                    .put("eventSource", "order-service");
//
//            // 트레이싱 헤더 추가
//            for (Map.Entry<String, String> entry : headerMap.entrySet()) {
//                headers.put(entry.getKey(), entry.getValue());
//            }
//
//            outboxRepo.save(OutboxEvent.builder()
//                    .aggregateType("ORDER")
//                    .aggregateId(orderId.toString())
//                    .type("OrderApproved")
//                    .payload(payload)
//                    .headers(headers)
//                    .published(false)
//                    .build());
//        } catch (Exception e) { throw new RuntimeException(e); }
    }

    @Transactional
    public void cancel(UUID orderId, String reason) {
        Order order = orderRepo.findById(orderId).orElseThrow();
        order.setStatus(OrderStatus.CANCELLED);
        orderRepo.save(order);
//        try {
//            log.info("Creating order cancellation event with ID: {}, reason: {}", orderId, reason);
//
//            var evt = OrderEvents.OrderCancelled.builder()
//                    .orderId(orderId).reason(reason).cancelledAt(Instant.now()).build();
//            JsonNode payload = om.valueToTree(evt); // evt -> JsonNode
//
//            // 현재 스팬의 컨텍스트를 헤더에 주입
//            Map<String, String> headerMap = new HashMap<>();
//
//            // 기존 방식의 traceId 헤더도 호환성을 위해 유지
//            String traceId = Optional.ofNullable(tracer.currentSpan())
//                    .map(span -> span.context().traceId())
//                    .orElse(null);
//
//            if (tracer.currentSpan() != null) {
//                propagator.inject(tracer.currentSpan().context(), headerMap,
//                        (carrier, key, value) -> carrier.put(key, value));
//                log.debug("Injected trace context into headers: {}", headerMap);
//            }
//
//            // 주입된 헤더를 JSON으로 변환
//            ObjectNode headers = om.createObjectNode()
//                    .put("traceId", traceId)
//                    .put("eventSource", "order-service");
//
//            // 트레이싱 헤더 추가
//            for (Map.Entry<String, String> entry : headerMap.entrySet()) {
//                headers.put(entry.getKey(), entry.getValue());
//            }
//            outboxRepo.save(OutboxEvent.builder()
//                    .aggregateType("ORDER")
//                    .aggregateId(orderId.toString())
//                    .type("OrderCancelled")
//                    .payload(payload)
//                    .headers(headers)
//                    .published(false)
//                    .build());
//        } catch (Exception e) { throw new RuntimeException(e); }
    }
}
