package com.kafka.learn.consumer;

import com.kafka.learn.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Kafka Consumer Service — listens to Order events from Kafka.
 *
 * Key concepts demonstrated:
 * - @KafkaListener with groupId, topic, and concurrency
 * - Manual offset acknowledgment (enable-auto-commit: false)
 * - Accessing Kafka metadata headers (partition, offset, key)
 * - Multiple consumers (one for each service)
 */
@Slf4j
@Service
public class OrderConsumerService {

    /**
     * CONSUMER 1: Inventory Service
     *
     * @KafkaListener binds this method to the orders-topic.
     * groupId = "inventory-group" — independent from other consumers.
     * concurrency = "3" — 3 threads, each reading 1 partition (matches our 3 partitions).
     */
    @KafkaListener(
            topics = "${app.kafka.topics.orders}",
            groupId = "inventory-group",
            concurrency = "3"   // 3 threads = 3 partitions → perfect parallelism
    )
    public void consumeForInventory(
            @Payload Order order,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            Acknowledgment acknowledgment) {

        try {
            log.info("📦 [INVENTORY] Received order | partition={} | offset={} | key={} | orderId={}",
                    partition, offset, key, order.getOrderId());

            // 🔧 Business logic: reserve inventory
            processInventory(order);

            // ✅ Manually commit offset ONLY after successful processing
            acknowledgment.acknowledge();
            log.info("✅ [INVENTORY] Offset committed for orderId={}", order.getOrderId());

        } catch (Exception e) {
            // ❌ Don't acknowledge → offset NOT committed → message re-delivered
            log.error("❌ [INVENTORY] Failed to process order: {} | error: {}",
                    order.getOrderId(), e.getMessage());
            // Spring will handle retry based on config
        }
    }

    /**
     * CONSUMER 2: Notification Service
     *
     * SAME topic, DIFFERENT groupId → reads ALL messages independently.
     * This consumer is completely independent from CONSUMER 1.
     */
    @KafkaListener(
            topics = "${app.kafka.topics.orders}",
            groupId = "notification-group"
    )
    public void consumeForNotification(ConsumerRecord<String, Order> record,
                                       Acknowledgment acknowledgment) {

        Order order = record.value();
        log.info("🔔 [NOTIFICATION] New order placed! customerId={} | product={} | amount={}",
                order.getCustomerId(),
                order.getProduct(),
                order.getTotalAmount());

        // 🔧 Business logic: send email/SMS
        sendNotification(order);

        acknowledgment.acknowledge();
    }

    // ── Simulated Business Logic ─────────────────────────────────────────────

    private void processInventory(Order order) {
        // Simulate inventory update
        log.info("  → Reserving {} units of '{}' for customer {}",
                order.getQuantity(), order.getProduct(), order.getCustomerId());
        // In real app: call InventoryRepository.decreaseStock(...)
    }

    private void sendNotification(Order order) {
        // Simulate notification
        log.info("  → Sending email to customer {} for order {}",
                order.getCustomerId(), order.getOrderId());
        // In real app: call EmailService.sendOrderConfirmation(...)
    }
}
