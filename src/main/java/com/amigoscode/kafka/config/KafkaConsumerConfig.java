package com.amigoscode.kafka.config;

import com.amigoscode.kafka.event.OrderCreatedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

/**
 * Configures the Kafka consumer.
 *
 * Key choices:
 *  - Key deserializer:   StringDeserializer  → keys come back as plain strings
 *  - Value deserializer: JsonDeserializer    → JSON bytes are mapped to OrderCreatedEvent objects
 *
 * The listener container factory is required by @KafkaListener methods.
 * ConcurrentKafkaListenerContainerFactory supports concurrent (multi-threaded) consumers.
 */
@Configuration
public class KafkaConsumerConfig {

    private final KafkaProperties kafkaProperties;

    public KafkaConsumerConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    /**
     * Raw consumer properties map.
     * Starts from Spring Boot's auto-configured properties (includes SASL/SSL)
     * and overrides the deserializers.
     */
    public Map<String, Object> consumerConfig() {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        // Tell JsonDeserializer which Java type to deserialize JSON into
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderCreatedEvent.class.getName());
        // Trust the package where our event class lives
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.amigoscode");

        return props;
    }

    @Bean
    public ConsumerFactory<String, OrderCreatedEvent> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfig());
    }

    /**
     * The listener container factory is the bridge between Spring's
     * @KafkaListener annotation and the underlying Kafka consumer.
     *
     * ConcurrentKafkaListenerContainerFactory allows you to set
     * factory.setConcurrency(N) to spin up N consumer threads,
     * each handling a separate partition.
     */
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, OrderCreatedEvent>>
        kafkaListenerContainerFactory(ConsumerFactory<String, OrderCreatedEvent> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }
}
