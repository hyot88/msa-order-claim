# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a microservices-based order and claim management system built with Spring Boot 3.3.3, Spring Cloud 2023.0.3, and Java 21. The system demonstrates event-driven architecture patterns including Outbox pattern, Saga pattern, and distributed tracing with Kafka as the messaging backbone.

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
2. `outbox-relay` module polls unpublished events from outbox table
3. Events are published to Kafka and marked as published
4. Distributed tracing context is preserved across this boundary

**Key files:**
- Service-side: `*/outbox/OutboxEvent.java`, `*/outbox/OutboxRepository.java`
- Relay: `outbox-relay/src/main/java/com/example/outbox/OutboxRelay.java`

### Event-Driven Communication
Services communicate asynchronously via Kafka topics:
- **order.events**: OrderCreated, OrderApproved, OrderCancelled
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

## Known Issues & Planned Features

See `fix.md` for architectural analysis and improvement suggestions.

Planned features (from README):
- Keycloak integration for authentication/authorization
- OAuth2/OpenID Connect implementation
- Enhanced monitoring with Prometheus and Grafana
- API documentation with SpringDoc OpenAPI
- Complete implementation of claim-service
