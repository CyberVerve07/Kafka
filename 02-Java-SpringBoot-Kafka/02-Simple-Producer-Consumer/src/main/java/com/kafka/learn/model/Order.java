package com.kafka.learn.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Order model — the message payload sent through Kafka.
 *
 * This is serialized to JSON by the producer
 * and deserialized from JSON by the consumer.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Order {

    private String orderId;
    private String customerId;
    private String product;
    private int quantity;
    private double totalAmount;
    private String status;          // PLACED, CONFIRMED, SHIPPED, DELIVERED
    private LocalDateTime createdAt;
}
