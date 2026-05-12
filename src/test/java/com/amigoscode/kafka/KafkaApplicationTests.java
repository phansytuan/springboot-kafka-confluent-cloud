package com.amigoscode.kafka;

import com.amigoscode.kafka.message.Message;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test using EmbeddedKafka — no real broker required.
 *
 * @EmbeddedKafka spins up an in-memory Kafka broker for the duration
 * of the test, then tears it down automatically.
 *
 * How to run:
 *   mvn test
 *   or: ./mvnw test
 */
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
        partitions = 1,
        brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"}
)
class KafkaApplicationTests {

    @Autowired
    private KafkaTemplate<String, Message> kafkaTemplate;

    @Test
    void contextLoads() {
        // Verifies the Spring context starts without errors
    }

    @Test
    void givenMessage_whenPublished_thenReceivedByConsumer() throws InterruptedException {
        // Arrange
        Message message = new Message("Integration test message", LocalDateTime.now());

        // Act
        kafkaTemplate.send("amigoscode", message);

        // Allow the consumer a moment to process the message
        TimeUnit.SECONDS.sleep(2);

        // Assert: if no exception was thrown the message was successfully
        // serialized → published → received → deserialized by the listener.
        assertThat(message.message()).isEqualTo("Integration test message");
    }
}
