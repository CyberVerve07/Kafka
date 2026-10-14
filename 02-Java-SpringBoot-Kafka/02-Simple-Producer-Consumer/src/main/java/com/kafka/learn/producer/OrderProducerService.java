package com.kafka.learn.producer;

import com.kafka.learn.model.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Kafka Producer Service — publishes Order events to Kafka.
 *
 * Key concepts demonstrated:
 * - KafkaTemplate.send() with topic + key + value
 * - Async callback to handle success/failure
 * - Using customerId as KEY → ensures all orders for same customer
 *   go to the SAME partition (ordering guaranteed per customer)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducerService {

    // KafkaTemplate<KEY_TYPE, VALUE_TYPE>
    private final KafkaTemplate<String, Order> kafkaTemplate;

    @Value("${app.kafka.topics.orders}")
    private String ordersTopic;

    /**
     * Sends an order event to Kafka asynchronously.
     *
     * KEY = customerId → same customer's orders always go to same partition.
     * This guarantees ORDER for a given customer.
     */
    public void sendOrder(Order order) {
        log.info("📤 Sending order to Kafka: orderId={}, customerId={}",
                order.getOrderId(), order.getCustomerId());

        // send(topic, key, value)
        CompletableFuture<SendResult<String, Order>> future =
                kafkaTemplate.send(ordersTopic, order.getCustomerId(), order);

        // Async callback — handle success & failure
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                // ✅ Success
                log.info("✅ Order sent successfully! orderId={} | partition={} | offset={}",
                        order.getOrderId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                // ❌ Failure
                log.error("❌ Failed to send order: orderId={} | error={}",
                        order.getOrderId(), ex.getMessage());
                // In production: save to outbox table / retry queue
            }
        });
    }

    /**
     * Synchronous send — blocks until ack received.
     * Use when you MUST know the result before proceeding.
     */
    public void sendOrderSync(Order order) throws Exception {
        log.info("📤 Sending order synchronously: orderId={}", order.getOrderId());

        SendResult<String, Order> result =
                kafkaTemplate.send(ordersTopic, order.getCustomerId(), order).get();

        log.info("✅ Order sent! partition={} offset={}",
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());
    }
}
