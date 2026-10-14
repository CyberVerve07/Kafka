package com.kafka.learn.config;

import com.kafka.learn.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Error Handling Configuration for Kafka Consumers.
 *
 * Strategy:
 * 1. Retry 3 times (with 1 second between retries)
 * 2. If still failing → send to Dead Letter Topic (DLT)
 * 3. DLT topic = original topic + ".DLT"  (e.g. "orders-topic.DLT")
 *
 * Flow:
 * Message → Consumer fails → Retry 1 → Retry 2 → Retry 3 → DLT
 */
@Slf4j
@Configuration
public class KafkaErrorHandlerConfig {

    /**
     * DefaultErrorHandler with:
     * - FixedBackOff: 3 retries, 1 second apart
     * - DeadLetterPublishingRecoverer: failed messages → DLT
     */
    @Bean
    public CommonErrorHandler errorHandler(KafkaTemplate<String, Order> kafkaTemplate) {

        // After 3 retries, publish to DLT
        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(kafkaTemplate,
                        (record, ex) -> {
                            log.error("💀 Moving to DLT after 3 retries | topic={} | key={} | error={}",
                                    record.topic(), record.key(), ex.getMessage());
                            // DLT topic = original.topic + ".DLT"
                            return new org.apache.kafka.common.TopicPartition(
                                    record.topic() + ".DLT", record.partition()
                            );
                        });

        // 3 retries, 1000ms (1 second) between each
        FixedBackOff backOff = new FixedBackOff(1000L, 3L);

        return new DefaultErrorHandler(recoverer, backOff);
    }
}
