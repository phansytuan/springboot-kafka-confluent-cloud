package com.amigoscode.kafka.event;

import java.time.LocalDateTime;

/**
 * A real domain event representing an order creation.
 *
 * This is the type of structured event you publish to Kafka in production
 * instead of raw strings.
 */
public class OrderCreatedEvent {

    private Long orderId;
    private String customerName;
    private Double amount;
    private LocalDateTime createdAt;

    public OrderCreatedEvent() {
    }

    public OrderCreatedEvent(Long orderId, String customerName, Double amount, LocalDateTime createdAt) {
        this.orderId = orderId;
        this.customerName = customerName;
        this.amount = amount;
        this.createdAt = createdAt;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public String toString() {
        return "OrderCreatedEvent{" +
                "orderId=" + orderId +
                ", customerName='" + customerName + '\'' +
                ", amount=" + amount +
                ", createdAt=" + createdAt +
                '}';
    }
}
