package com.kafka.learn.producer;

import com.kafka.learn.model.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Transactional Kafka Producer — Exactly-Once Semantics (EOS).
 *
 * Use case: Payment processing where you CANNOT have duplicate messages.
 *
 * Config required in application.yml:
 * spring.kafka.producer.transaction-id-prefix: tx-
 *
 * What "exactly-once" means:
 * - Even if producer retries, the message appears EXACTLY ONCE in the topic
 * - No duplicates, no loss
 *
 * Trade-off: Lower throughput compared to at-least-once.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionalOrderProducer {

    private final KafkaTemplate<String, Order> kafkaTemplate;

    @Value("${app.kafka.topics.orders}")
    private String ordersTopic;

    /**
     * Sends multiple messages in a SINGLE Kafka transaction.
     * Either ALL succeed or NONE are committed.
     *
     * @Transactional ensures atomicity.
     */
    @Transactional("kafkaTransactionManager")
    public void sendOrdersInTransaction(Order order1, Order order2) {
        log.info(" Starting Kafka transaction...");

        try {
            kafkaTemplate.send(ordersTopic, order1.getCustomerId(), order1);
            log.info("  → Sent order1: {}", order1.getOrderId());

            // Simulate some processing
            // If this throws → BOTH messages are rolled back (not committed)
            validateOrder(order2);

            kafkaTemplate.send(ordersTopic, order2.getCustomerId(), order2);
            log.info("  → Sent order2: {}", order2.getOrderId());

            log.info(" Transaction committed — both orders published!");

        } catch (Exception e) {
            log.error(" Transaction rolled back — neither order published! Error: {}", e.getMessage());
            throw e; // re-throw so @Transactional rolls back
        }
    }

    private void validateOrder(Order order) {
        if (order.getTotalAmount() <= 0) {
            throw new IllegalArgumentException("Invalid order amount: " + order.getTotalAmount());
        }
    }
}
