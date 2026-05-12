# Spring Boot + Apache Kafka + Confluent Cloud

A production-ready Spring Boot application demonstrating event-driven architecture with Apache Kafka on **Confluent Cloud**.

This project goes beyond "Hello World" — it uses structured **JSON events** (`OrderCreatedEvent`) rather than raw strings, and connects to a real managed Kafka cluster with SASL/SSL authentication.

---

## Architecture

```
POST /api/v1/orders
        ↓
OrderController
        ↓
KafkaTemplate<String, OrderCreatedEvent>
        ↓
Confluent Cloud Kafka (topic: orders)
        ↓
KafkaListeners (@KafkaListener)
        ↓
Process event (log, email, inventory, etc.)
```

---

## Project Structure

```
kafka-demo/
├── application.properties              # Confluent Cloud credentials (gitignored)
├── docker-compose.yml                  # Local Kafka for dev (optional)
├── pom.xml
└── src/
    └── main/
        ├── java/com/amigoscode/kafka/
        │   ├── KafkaApplication.java          # Entry point + startup producer
        │   ├── config/
        │   │   ├── KafkaTopicConfig.java       # Declares the "orders" topic
        │   │   ├── KafkaProducerConfig.java    # Producer factory + KafkaTemplate
        │   │   └── KafkaConsumerConfig.java    # Consumer factory + listener factory
        │   ├── event/
        │   │   └── OrderCreatedEvent.java     # Real domain event DTO
        │   ├── listener/
        │   │   └── KafkaListeners.java        # @KafkaListener consumer
        │   └── controller/
        │       └── OrderController.java       # REST endpoints for orders
        └── resources/
            └── application.properties         # Confluent config template
```

---

## Prerequisites

| Tool          | Version  | Notes |
|---------------|----------|-------|
| Java          | 17+      | Spring Boot 3.2 target |
| Maven         | 3.8+     | Build tool |
| Confluent Cloud | Free   | Managed Kafka cluster |

---

## Configuration

### 1. Confluent Cloud Setup

Create a cluster on [Confluent Cloud](https://confluent.cloud/) and grab:
- **Bootstrap Server** (e.g., `pkc-921jm.us-east-2.aws.confluent.cloud:9092`)
- **API Key**
- **API Secret**

### 2. Set Credentials

Edit `application.properties` in the project root:

```properties
spring.kafka.properties.sasl.mechanism=PLAIN
spring.kafka.bootstrap-servers=YOUR_BOOTSTRAP_SERVER:9092
spring.kafka.properties.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='YOUR_KEY' password='YOUR_SECRET';
spring.kafka.properties.security.protocol=SASL_SSL
spring.kafka.properties.session.timeout.ms=45000
```

> **Security Warning:** `application.properties` at the project root is `.gitignore`d. Never commit API secrets to Git.

---

## Quick Start

### Build

```bash
mvn clean package -DskipTests
```

### Run

**Option A: With credentials in `application.properties` (project root)**
```bash
mvn spring-boot:run
```

**Option B: Pass credentials via JVM argument**
```bash
java "-Dspring.kafka.properties.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='YOUR_KEY' password='YOUR_SECRET';" -jar target/kafka-demo-0.0.1-SNAPSHOT.jar
```

On startup, the app will:
1. Authenticate to Confluent Cloud via SASL_SSL.
2. Auto-create the `orders` topic (if not exists).
3. Send a startup `OrderCreatedEvent` to the topic.
4. The consumer will immediately receive and log it.

**Expected output:**
```
[KafkaApplication] Startup OrderCreatedEvent sent to topic 'orders'
=========================================
Listener received: OrderCreatedEvent{orderId=1, customerName='Diep', amount=120.5, ...}
Order ID: 1
Customer: Diep
Amount: $120.5
Created at: 2026-05-12T10:46:20.289372483
=========================================
```

---

## REST API

### Publish an Order Event

```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": 102,
    "customerName": "Tuan",
    "amount": 250.0
  }'
```

**Response:**
```
OrderCreatedEvent published to topic 'orders'
```

**Console output:**
```
Order event sent to partition 2 at offset 5
=========================================
Listener received: OrderCreatedEvent{orderId=102, customerName='Tuan', amount=250.0, ...}
Order ID: 102
Customer: Tuan
Amount: $250.0
Created at: ...
=========================================
```

---

## Key Concepts

### Event-Driven Architecture
Unlike raw string messages, this app uses **structured domain events** (`OrderCreatedEvent`). In production Kafka, you almost always send:
- `UserCreatedEvent`
- `PaymentCompletedEvent`
- `OrderCancelledEvent`

Kafka acts as the **event backbone** between services.

### Serialization (Producer)
`OrderCreatedEvent` objects are serialized to JSON bytes using `JsonSerializer`. The key is serialized with `StringSerializer`.

### Deserialization (Consumer)
`JsonDeserializer` converts JSON bytes back into `OrderCreatedEvent` objects. The target type and trusted packages are configured via properties:
```properties
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
spring.kafka.consumer.properties.spring.json.value.default.type=com.amigoscode.kafka.event.OrderCreatedEvent
spring.kafka.consumer.properties.spring.json.trusted.packages=com.amigoscode
```

### Consumer Groups
The listener uses `groupId = "springboot-group-1"`. If you run multiple app instances, Kafka distributes partitions among them — each message is processed by **exactly one** instance.

Use **different group IDs** if you want independent consumers (e.g., analytics service + notification service both need all events).

### KafkaTemplate
Spring's high-level abstraction for sending events. The `send()` method returns a `CompletableFuture<SendResult>` for async result handling (partition, offset, timestamp).

---

## Verify in Confluent Cloud UI

1. Go to **Topics** → `orders`
2. Click **Messages**
3. You should see:
   - **Key:** `null` (or order ID if you set it)
   - **Value:** JSON payload like `{"orderId":102,"customerName":"Tuan","amount":250.0,"createdAt":"2026-05-12T10:30:00"}`
   - **Timestamp**

---

## Running Tests

```bash
mvn test
```

Tests use `@EmbeddedKafka` — no real broker or internet required.

---

## Roadmap

### ✅ Foundation (Current)
- [x] Producer / Consumer with Confluent Cloud
- [x] JSON event serialization (`OrderCreatedEvent`)
- [x] Topic creation
- [x] Consumer groups
- [x] SASL/SSL authentication

### 🔜 Production Basics
- [ ] Multiple consumer instances (load balancing)
- [ ] Retry mechanism (`@RetryableTopic`)
- [ ] Dead Letter Queue (DLQ)
- [ ] Idempotency
- [ ] Exactly-once semantics

### 🔜 Real Engineering
- [ ] Schema Registry (Avro / Protobuf)
- [ ] Event versioning
- [ ] Kafka Streams
- [ ] Saga pattern / CQRS

---

## Troubleshooting

### Port 8080 is already in use
```bash
lsof -ti:8080 | xargs kill -9
```

### Authentication Failed
- Double-check your API Key / Secret in `application.properties`
- Verify `bootstrap-servers` matches your Confluent Cloud cluster
- Ensure `security.protocol=SASL_SSL` is set

### Consumer receives String instead of OrderCreatedEvent
Make sure `KafkaConsumerConfig` configures `JsonDeserializer.VALUE_DEFAULT_TYPE` and `TRUSTED_PACKAGES` in the properties map, and the factory bean is named `kafkaListenerContainerFactory`.

### No messages in Confluent Cloud UI
- Check that the topic name matches (`orders`)
- Verify the consumer group isn't reading from the latest offset only
- Try setting `auto.offset.reset=earliest` if you want to see old messages

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Framework | Spring Boot 3.2 |
| Language | Java 17 |
| Messaging | Spring for Apache Kafka |
| Serialization | Jackson (JSON) |
| Cloud Kafka | Confluent Cloud |
| Authentication | SASL/SSL (PLAIN) |

---

## Local Development (Optional)

If you prefer a local Kafka instead of Confluent Cloud:

```bash
docker compose up -d
```

Then update `application.properties`:
```properties
spring.kafka.bootstrap-servers=localhost:9092
# Remove SASL/SSL lines
```

---

## License

MIT
