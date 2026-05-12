package com.amigoscode.kafka.config;

import com.amigoscode.kafka.event.OrderCreatedEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

/**
 * Configures the Kafka producer.
 *
 * Key choices:
 *  - Key serializer:   StringSerializer   → topic keys are plain strings
 *  - Value serializer: JsonSerializer     → OrderCreatedEvent objects are JSON-encoded
 *
 * KafkaTemplate is the primary Spring abstraction for sending events;
 *  inject it wherever you need to publish OrderCreatedEvents.
 */
@Configuration
public class KafkaProducerConfig {

    private final KafkaProperties kafkaProperties;

    public KafkaProducerConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    /**
     * Raw producer properties map.
     * Starts from Spring Boot's auto-configured properties (includes SASL/SSL)
     * and overrides the serializers.
     */
    public Map<String, Object> producerConfig() {
        Map<String, Object> props = kafkaProperties.buildProducerProperties(null);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class); // Keys are plain strings (e.g. order IDs)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class); // Values are Java objects serialized to JSON bytes

        return props;
    }

    @Bean
    public ProducerFactory<String, OrderCreatedEvent> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfig());
    }

    /**
     * KafkaTemplate wraps the ProducerFactory and exposes a high-level
     * send() API. Autowire this bean anywhere you need to publish events.
     */
    @Bean
    public KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate(
            ProducerFactory<String, OrderCreatedEvent> producerFactory
    ) {
        return new KafkaTemplate<>(producerFactory);
    }
}
