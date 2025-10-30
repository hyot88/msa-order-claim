package com.example.outbox;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.*;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Configuration
@Slf4j
public class KafkaConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        log.info("Creating Kafka producer factory with bootstrap servers: {}", bootstrapServers);
        return new DefaultKafkaProducerFactory<>(
                Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                        // 멱등성/안정성
                        ProducerConfig.ACKS_CONFIG, "all",
                        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000,
                        ProducerConfig.RETRIES_CONFIG, 10,
                        ProducerConfig.LINGER_MS_CONFIG, 10
                )
        );
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        log.info("Creating KafkaTemplate bean");
        KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory());
        log.info("KafkaTemplate bean created successfully");
        return template;
    }
}
