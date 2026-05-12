package com.amigoscode.kafka.message;

import java.time.LocalDateTime;

/**
 * Domain object that gets serialized to JSON and published to Kafka.

 * Using a Java record keeps this immutable and concise.
 * The JsonSerializer/JsonDeserializer pair handles conversion to/ from bytes.
 */
public record Message(
        String message,
        LocalDateTime createdAt
) {
}
