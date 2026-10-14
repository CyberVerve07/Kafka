package com.kafka.learn.controller;

import com.kafka.learn.model.Order;
import com.kafka.learn.producer.OrderProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * REST Controller — trigger point for sending Kafka messages.
 *
 * Test with curl:
 *
 * POST http://localhost:8081/api/orders
 * Body: { "customerId": "cust-001", "product": "iPhone 15", "quantity": 1, "totalAmount": 75000 }
 *
 * POST http://localhost:8081/api/orders/bulk?count=10
 * → Sends 10 test orders rapidly to see batching in action
 */
@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderProducerService producerService;

    /**
     * Place a single order — publishes to Kafka.
     */
    @PostMapping
    public ResponseEntity<String> placeOrder(@RequestBody Order order) {
        // Auto-fill fields if not provided
        if (order.getOrderId() == null) {
            order.setOrderId(UUID.randomUUID().toString());
        }
        order.setStatus("PLACED");
        order.setCreatedAt(LocalDateTime.now());

        producerService.sendOrder(order);

        return ResponseEntity.ok("Order placed! ID: " + order.getOrderId());
    }

    /**
     * Send bulk orders — useful to see Kafka partitioning and batching.
     */
    @PostMapping("/bulk")
    public ResponseEntity<String> bulkOrders(@RequestParam(defaultValue = "5") int count) {
        log.info("🚀 Sending {} bulk orders to Kafka...", count);

        for (int i = 1; i <= count; i++) {
            Order order = Order.builder()
                    .orderId(UUID.randomUUID().toString())
                    .customerId("cust-" + (i % 3))        // 3 customers → keys spread across partitions
                    .product("Product-" + i)
                    .quantity(i)
                    .totalAmount(i * 999.99)
                    .status("PLACED")
                    .createdAt(LocalDateTime.now())
                    .build();

            producerService.sendOrder(order);
        }

        return ResponseEntity.ok("Sent " + count + " orders to Kafka!");
    }

    /**
     * Simple health check.
     */
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Kafka Demo is running! 🚀");
    }
}
