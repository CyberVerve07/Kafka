package com.kafka.learn.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Kafka Topic Configuration.
 *
 * Spring Kafka auto-creates topics on startup if they don't exist.
 * In production: disable auto.create.topics.enable on broker,
 * and use this bean to manage topics declaratively.
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${app.kafka.topics.orders}")
    private String ordersTopic;

    @Value("${app.kafka.topics.notifications}")
    private String notificationsTopic;

    /**
     * Orders topic — 3 partitions, replication factor 1 (for local dev).
     * In production: replication-factor = 3, min.insync.replicas = 2
     */
    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name(ordersTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * Notifications topic — 1 partition (order matters here).
     */
    @Bean
    public NewTopic notificationsTopic() {
        return TopicBuilder.name(notificationsTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
