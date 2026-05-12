package com.amigoscode.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the Kafka topic(s) this application uses.
 *
 * Spring Boot will automatically create the topic on startup
 *  if it doesn't already exist on the broker.
 */
@Configuration
public class KafkaTopicConfig {

    /**
     * Creates a topic named "amigoscode" with default settings
     * (1 partition, replication factor 1 — suitable for local dev).
     *
     * For production, increase partitions and replication:
     *   TopicBuilder.name("amigoscode")
     *       .partitions(6)
     *       .replicas(3)
     *       .build();
     */
    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder
                .name("orders")
                .build();
    }
}
