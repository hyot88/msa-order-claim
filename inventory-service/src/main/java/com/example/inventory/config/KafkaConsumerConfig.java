package com.example.inventory.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.util.function.BiFunction;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> tpl,
                                            @Value("${kafka.retry.max:3}") int maxRetries,
                                            @Value("${kafka.retry.initial:500}") long initialBackoffMs) {

        // DLT 라우팅 규칙: {topic}.DLT
        BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> dest = (rec, ex) -> {
            String dlt = rec.topic() + ".DLT";
            return new TopicPartition(dlt, rec.partition());
        };

        var recoverer = new DeadLetterPublishingRecoverer(tpl, dest);

        var backoff = new ExponentialBackOffWithMaxRetries(maxRetries);
        backoff.setInitialInterval(initialBackoffMs);
        backoff.setMultiplier(2.0);
        backoff.setMaxInterval(5_000);

        var handler = new DefaultErrorHandler(recoverer, backoff);

        // (선택) 무시할 예외 등록 가능
        // handler.addNotRetryableExceptions(ValidationException.class);

        return handler;
    }
}
