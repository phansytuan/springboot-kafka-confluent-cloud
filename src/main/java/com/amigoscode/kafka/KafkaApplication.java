package com.amigoscode.kafka;

import com.amigoscode.kafka.event.OrderCreatedEvent;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.LocalDateTime;

@SpringBootApplication
@EnableKafka
public class KafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaApplication.class, args);
    }

    /**
     * Sends a startup OrderCreatedEvent to the Kafka topic when the application boots.
     * Demonstrates using KafkaTemplate to produce a structured event (JSON-serialized).
     */
    @Bean
    CommandLineRunner commandLineRunner(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
        return args -> {
            OrderCreatedEvent startupEvent = new OrderCreatedEvent(
                    1L,
                    "Diep",
                    120.5,
                    LocalDateTime.now()
            );
            kafkaTemplate.send("orders", startupEvent);
            System.out.println("[KafkaApplication] Startup OrderCreatedEvent sent to topic 'orders'");
        };
    }
}
