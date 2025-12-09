# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a microservices-based order and claim management system built with Spring Boot 3.3.3, Spring Cloud 2023.0.3, and Java 21. The system demonstrates event-driven architecture patterns including Outbox pattern, Saga pattern (choreography-based), and distributed tracing with Kafka as the messaging backbone.

**Key Design Decisions:**
- Choreography-based Saga (not orchestration) - services react to events independently
- Transactional Outbox pattern for guaranteed event delivery
- One PostgreSQL database per service (polyglot persistence ready)
- Distributed tracing via Micrometer + Zipkin with context propagation through Kafka headers
- Idempotency via ProcessedMessage table to handle at-least-once delivery

**Code Documentation:**
All Java, YAML, and Gradle files contain extensive **Korean language comments** explaining:
- Why architectural patterns are used (not just what they do)
- Step-by-step flow diagrams in comments
- Trade-offs and design decisions
- Examples of how to use each component
- This makes the codebase highly accessible for Korean-speaking developers new to MSA patterns

## Build Commands

```bash
# Build all modules
./gradlew build

# Build specific module
./gradlew :order-service:build
./gradlew :payment-service:build
./gradlew :inventory-service:build
./gradlew :claim-service:build
./gradlew :api-gateway:build
./gradlew :discovery:build
./gradlew :config-server:build
./gradlew :outbox-relay:build

# Clean build
./gradlew clean build

# Run tests
./gradlew test

# Run tests for specific module
./gradlew :order-service:test

# Run single test class
./gradlew :order-service:test --tests "ClassName"

# Run application (from service directory)
./gradlew :order-service:bootRun
```

## Infrastructure Setup

Infrastructure services (Kafka, Zookeeper, PostgreSQL, Zipkin, Kafka UI) are managed via Docker Compose:

```bash
# Start all infrastructure services
docker-compose -f docker/docker-compose.yml up -d

# Stop infrastructure services
docker-compose -f docker/docker-compose.yml down

# View logs
docker-compose -f docker/docker-compose.yml logs -f [service-name]
```

### Infrastructure Components
- **Kafka**: localhost:29092 (external), kafka:9092 (internal)
- **Zookeeper**: localhost:2181
- **Kafka UI**: http://localhost:8085
- **Zipkin**: http://localhost:9411
- **PostgreSQL (order-service)**: localhost:5433 (user: order, db: orderdb)
- **PostgreSQL (claim-service)**: localhost:5434 (user: claim, db: claimdb)

## Service Startup Order

Services must be started in the following order to ensure proper initialization:

1. **Infrastructure** (via Docker Compose)
2. **discovery** (Eureka Server on port 8761)
3. **config-server** (on port 8888)
4. **Core services** (order-service:9001, payment-service, inventory-service)
5. **outbox-relay** (polls outbox table and publishes to Kafka)
6. **api-gateway** (on port 8080)

## Architecture Patterns

### Outbox Pattern
All services that publish events use the Outbox pattern to guarantee at-least-once delivery:
1. Service writes domain entity + outbox event in single transaction
2. `outbox-relay` module polls unpublished events from outbox table every 500ms (configurable via `relay.pollMs`)
3. Uses `FOR UPDATE SKIP LOCKED` to support multiple relay instances without duplicate processing
4. Events are published to Kafka and marked as published via JPA dirty checking
5. Distributed tracing context is preserved from service → outbox → kafka → consumer

**Key files:**
- Service-side: `*/outbox/OutboxEvent.java`, `*/outbox/OutboxRepository.java`
- Relay: `outbox-relay/src/main/java/com/example/outbox/OutboxRelay.java`
- Publisher: `common-lib/src/main/java/com/example/common/outbox/OutboxEventPublisher.java`

**Configuration:**
- `relay.pollMs`: Polling interval in milliseconds (default: 500)
- `relay.batchSize`: Max events per batch (default: 50)

### Saga Pattern (Choreography-Based)
The order fulfillment flow uses choreography-based saga where services react to events:

**Happy Path:**
1. order-service: Creates order (PENDING) → publishes `OrderCreated`
2. inventory-service: Consumes `OrderCreated` → checks stock → publishes `InventoryReserved`
3. payment-service: Consumes `InventoryReserved` → processes payment → publishes `PaymentAuthorized`
4. order-service: Consumes `PaymentAuthorized` → updates status to APPROVED → publishes `OrderApproved`

**Failure Path (Compensation):**
- If inventory fails: inventory-service publishes `InventoryReservationFailed` → order-service cancels order
- If payment fails: payment-service publishes `PaymentFailed` → order-service cancels order and inventory-service releases stock

**Key implementation points:**
- Each consumer uses idempotency checks via ProcessedMessage table (topic, partition, offset as unique key)
- Compensation is implicit - services listen for failure events and react accordingly
- No centralized orchestrator - each service decides its actions based on events

### Event-Driven Communication
Services communicate asynchronously via Kafka topics:
- **order.events**: OrderCreated, OrderApproved, OrderCancelled
- **inventory.events**: InventoryReserved, InventoryReservationFailed
- **payment.events**: PaymentAuthorized, PaymentFailed
- Events defined in `common-lib/src/main/java/com/example/common/events/OrderEvents.java`

### Idempotency
Services use `ProcessedMessage` table to track consumed message IDs and prevent duplicate processing:
- Check `*/idempotency/ProcessedMessage.java` and `*/idempotency/ProcessedMessageRepository.java`

### Distributed Tracing
All services propagate traceId through:
- HTTP requests (via Spring Cloud Sleuth/Micrometer)
- Kafka messages (headers injected by propagator)
- Outbox relay (preserves trace context from outbox headers to Kafka headers)

TraceIds are viewable in Zipkin UI at http://localhost:9411

### Circuit Breaker
API Gateway implements Resilience4j circuit breaker for order-service routes:
- Configuration in `api-gateway/src/main/resources/application.yml`
- Fallback endpoints in API Gateway
- Pattern: CLOSED → OPEN (50% failure rate) → HALF_OPEN → CLOSED/OPEN

## Common Development Patterns

### Adding a New Event
1. Define event class in `common-lib/src/main/java/com/example/common/events/`
2. Producer: Save to Outbox table with event type and JSON payload
3. Configure topic routing in `outbox-relay/src/main/java/com/example/outbox/OutboxRelay.java`
4. Consumer: Create `@KafkaListener` method with idempotency check

### Adding a New Service
1. Add module to `settings.gradle`
2. Create `build.gradle` with Spring Boot plugin and dependencies
3. Implement outbox pattern if publishing events
4. Configure Eureka client, database, and Kafka in `application.yml`
5. Add route in `api-gateway/src/main/resources/application.yml`

## Database Schema Management

Services use `spring.jpa.hibernate.ddl-auto: update` for development. Schema changes are applied automatically on startup. For production, this should be changed to `validate` and use migration tools like Flyway or Liquibase.

## Configuration Management

Config Server is implemented but not actively used. Each service has local `application.yml` files. To centralize configuration:
1. Create config repository
2. Add `spring.config.import: "optional:configserver:http://localhost:8888"` to each service
3. Use `@RefreshScope` for dynamic config updates

## Monitoring

Services expose actuator endpoints:
- `/actuator/health` - Health check
- `/actuator/info` - Application info
- `/actuator/prometheus` - Prometheus metrics

Tracing probability is set to 1.0 (100%) for development.

## Module Structure

### common-lib
Shared event definitions and utilities used across all services. Does not run standalone.

### discovery (Eureka Server)
Service registry. All services register here and discover each other.

### config-server
Centralized configuration server (currently not actively used).

### api-gateway
Entry point for client requests. Routes to services via Eureka service names. Implements circuit breakers.

### order-service
Manages order lifecycle: creation, status updates. Publishes OrderCreated/OrderApproved/OrderCancelled events.

### payment-service
Processes payments. Consumes order events, performs payment logic, publishes results.

### inventory-service
Manages product inventory. Consumes order events, checks/reserves inventory.

### claim-service
Handles returns/cancellations (under development).

### outbox-relay
Standalone daemon that polls outbox tables and publishes events to Kafka. Critical for Outbox pattern reliability.

## Technology Stack

- **Language**: Java 21
- **Framework**: Spring Boot 3.3.3, Spring Cloud 2023.0.3
- **Messaging**: Apache Kafka
- **Database**: PostgreSQL (one per service)
- **Service Discovery**: Netflix Eureka
- **API Gateway**: Spring Cloud Gateway
- **Tracing**: Zipkin + Micrometer Tracing + Brave
- **Resilience**: Resilience4j
- **Build**: Gradle 8.x Multi-module
- **Infrastructure**: Docker Compose

## Testing & Debugging

### Testing Saga Flows
1. Create an order via API Gateway:
```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user123", "totalAmount": 1000}'
```

2. Check order status:
```bash
curl http://localhost:8080/orders/{orderId}
```

3. View event flow in Kafka UI: http://localhost:8085
   - Check `order.events`, `inventory.events`, `payment.events` topics
   - View message headers for traceId propagation

4. View distributed tracing in Zipkin: http://localhost:9411
   - Search by traceId or service name
   - Visualize complete saga flow across services

### Simulating Failures
Services have configurable failure rates for testing compensation flows:

**inventory-service** (`application.yml`):
```yaml
service:
  inventory: 0.1  # 10% chance of inventory reservation failure
```

**payment-service** (`application.yml`):
```yaml
service:
  payment: 0.1  # 10% chance of payment failure
```

Set these values to 0.0 for always-success or 1.0 for always-fail scenarios.

### Debugging Outbox Pattern
1. Check unpublished events:
```sql
SELECT * FROM outbox_event WHERE published = false ORDER BY created_at;
```

2. Check processed messages (idempotency):
```sql
SELECT * FROM processed_message ORDER BY processed_at DESC LIMIT 100;
```

3. Monitor outbox-relay logs for polling activity:
```bash
docker-compose -f docker/docker-compose.yml logs -f
# Then check outbox-relay service logs
```

### Common Issues

**Symptom**: Events not being consumed
- Check: Kafka is running (`docker ps | grep kafka`)
- Check: Consumer groups are active (Kafka UI → Consumers)
- Check: outbox-relay is running and processing events
- Check: Service logs for Kafka connection errors

**Symptom**: Duplicate message processing
- Check: ProcessedMessage table has unique constraint on (topic, partition, offset)
- Check: Consumer methods wrap idempotency check in try-catch
- Verify: Database transactions are properly configured

**Symptom**: Lost tracing context
- Check: Kafka message headers include `traceparent` or `b3` headers
- Check: OutboxEvent.headers contains trace context
- Verify: Propagator is correctly injecting/extracting context

**Symptom**: Outbox events stuck as unpublished
- Check: outbox-relay is running
- Check: Kafka broker is reachable from outbox-relay
- Check: `FOR UPDATE SKIP LOCKED` query is not timing out
- Try: Restart outbox-relay to retry failed events

## Known Issues & Planned Features

Planned features (from README):
- Keycloak integration for authentication/authorization
- OAuth2/OpenID Connect implementation
- Enhanced monitoring with Prometheus and Grafana
- API documentation with SpringDoc OpenAPI
- Complete implementation of claim-service

Known limitations:
- No orchestration-based saga implementation (only choreography)
- No event versioning strategy (breaking changes require coordinated deployment)
- No dead letter queue for permanently failed events
- Circuit breaker only on API Gateway, not between services
- No rate limiting or backpressure handling
