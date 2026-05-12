package com.amigoscode.kafka.controller;

import com.amigoscode.kafka.event.OrderCreatedEvent;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

/**
 * REST API for publishing OrderCreatedEvent to Kafka.
 *
 * Example:
 *   POST http://localhost:8080/api/v1/orders
 *   {
 *     "orderId": 101,
 *     "customerName": "Diep",
 *     "amount": 120.5
 *   }
 */
@RestController
@RequestMapping("api/v1/orders")
public class OrderController {

    private static final String TOPIC = "orders";

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public OrderController(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public ResponseEntity<String> publish(@RequestBody OrderCreatedEvent request) {
        OrderCreatedEvent event = new OrderCreatedEvent(
                request.getOrderId(),
                request.getCustomerName(),
                request.getAmount(),
                LocalDateTime.now()
        );

        CompletableFuture<SendResult<String, OrderCreatedEvent>> future = kafkaTemplate.send(TOPIC, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                System.err.println("Failed to send order event: " + ex.getMessage());
            } else {
                System.out.println("Order event sent to partition "
                        + result.getRecordMetadata().partition()
                        + " at offset "
                        + result.getRecordMetadata().offset());
            }
        });

        return ResponseEntity.ok("OrderCreatedEvent published to topic '" + TOPIC + "'");
    }
}
