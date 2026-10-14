package com.kafka.learn;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Main entry point for the Kafka Spring Boot Demo application.
 *
 * How to run:
 * 1. Start Kafka: docker-compose up -d  (from 01-Setup-and-Configuration/)
 * 2. Run: mvn spring-boot:run
 * 3. Test: POST http://localhost:8081/api/orders
 */
@SpringBootApplication
public class KafkaDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaDemoApplication.class, args);
    }
}
