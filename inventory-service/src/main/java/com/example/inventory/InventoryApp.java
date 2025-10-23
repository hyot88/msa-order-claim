package com.example.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class InventoryApp {
    public static void main(String[] args) { SpringApplication.run(InventoryApp.class, args); }
}
