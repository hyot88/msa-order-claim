package com.example.outbox;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication(scanBasePackages = {"com.example.outbox", "com.example.common"})
@EnableJpaRepositories(basePackages = {"com.example.outbox", "com.example.common"})
@EntityScan(basePackages = {"com.example.outbox", "com.example.common"})
public class OutboxRelayApp {
    public static void main(String[] args) {
        SpringApplication.run(OutboxRelayApp.class, args);
    }
}