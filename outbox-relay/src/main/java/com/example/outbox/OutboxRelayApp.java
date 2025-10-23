package com.example.outbox;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OutboxRelayApp {
    public static void main(String[] args) {
        SpringApplication.run(OutboxRelayApp.class, args);
    }
}