package com.example.outbox;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
@EnableScheduling
@RequiredArgsConstructor
public class OutboxRelay {

    private final OutboxEventRepository repo;
    private final KafkaTemplate<String, String> kafka;

    @Value("${relay.batchSize:50}") int batchSize;

    @Scheduled(fixedDelayString = "${relay.pollMs:500}")
    @Transactional
    public void pump() {
        // 1) unpublished 행들을 락걸고 가져오기
        List<OutboxEvent> events = repo.lockAndFetchRaw(batchSize);
        if (events.isEmpty()) return;

        // 3) Kafka 전송 & published=true
        for (OutboxEvent e : events) {
            String topic = switch (e.getType()) {
                case "OrderCreated", "OrderApproved", "OrderCancelled" -> "order.events";
                default -> "order.events";
            };

            // Spring Kafka 3.x: send()는 CompletableFuture 반환
            kafka.send(topic, e.getAggregateId(), e.getPayload().toString()).join();

            // JPA가 dirty checking으로 UPDATE 수행
            e.setPublished(true);
        }

        // 4) 트랜잭션 커밋 시점에 일괄 flush
    }
}
