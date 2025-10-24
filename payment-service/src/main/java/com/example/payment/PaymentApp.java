package com.example.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class PaymentApp {
    public static void main(String[] args) { SpringApplication.run(PaymentApp.class, args); }
}
