package com.amigoscode.kafka.listener;

import com.amigoscode.kafka.event.OrderCreatedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer — listens to the "orders" topic and processes
 *  every incoming OrderCreatedEvent object.
 *
 * groupId = "springboot-group-1":
 *   - All instances sharing the same groupId form a consumer group.
 *   - Kafka distributes partitions across instances in the group,
 *     so each message is processed by exactly ONE instance.
 *   - Use a DIFFERENT groupId if you want every consumer to receive
 *     every message independently (e.g., separate analytics + logging services).
 *
 * Scaling tip:
 *   If the topic has N partitions, you can run up to N consumer instances
 *    in the same group and each will handle a dedicated partition in parallel.
 */
@Component
public class KafkaListeners {

    @KafkaListener(
            id = "myConsumer",
            topics = "orders",
            groupId = "springboot-group-1"
    )
    void listener(OrderCreatedEvent event) {
        System.out.println("=========================================");
        System.out.println("Listener received: " + event);
        System.out.println("Order ID: " + event.getOrderId());
        System.out.println("Customer: " + event.getCustomerName());
        System.out.println("Amount: $" + event.getAmount());
        System.out.println("Created at: " + event.getCreatedAt());
        System.out.println("=========================================");
    }
}
