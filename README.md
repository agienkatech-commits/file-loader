# File Loader Microservice

A robust, modern Spring Boot microservice for loading files from blob storage and publishing events to Kafka. Built with Java 25, Spring Cloud Stream, and following functional programming best practices.

## Features

- **File Polling**: Scheduled polling of blob storage directories for new files
- **Robust File Handling**: Atomic file moves with fallback for blob storage compatibility
- **File Stability Check**: Configurable delay to ensure files are fully written before processing
- **Kafka Integration**: Publishes file loaded events to Kafka topics
- **Error Channel**: Dedicated error notification channel for monitoring and debugging
- **Retry Logic**: Configurable retry attempts for transient failures
- **Parallel Processing**: Processes multiple directories in parallel
- **Kafka Consumer Ready**: Easily adaptable for Kafka-consumer to file-processor pattern

## Technology Stack

- Java 25 with modern features (var keyword, switch expressions, records)
- Spring Boot 3.5.6
- Spring Cloud Stream 2025.0.0
- Gradle 8.14.3
- Lombok for boilerplate reduction

## Architecture

The service follows a clean, modular architecture:

- **FileProcessingService**: Orchestrates directory scanning and file discovery
- **FileProcessor**: Handles individual file processing with retry logic
- **FilesHelper**: Utility class with file operations (atomic moves, stability checks)
- **FileNotificationProducer**: Publishes successful file processing events
- **ErrorNotificationProducer**: Publishes error events to dedicated error channel
- **FileLoaderProperties**: Externalized configuration with sensible defaults

## Configuration

Key configuration properties in `application.properties`:

```properties
# Kafka broker
spring.cloud.stream.kafka.binder.brokers=localhost:9092

# File loader settings
file-loader.polling-interval=PT3S
file-loader.new-subdirectory=new
file-loader.loading-subdirectory=loading
file-loader.loaded-subdirectory=loaded
file-loader.retry-attempts=3
file-loader.retry-delay=PT2S
file-loader.file-stability-check-delay=PT1S

# Directory to Kafka topic mappings
file-loader.source-directories.[/mnt/blob-storage/container1/directory1]=fileNotification1-out-0
file-loader.source-directories.[/mnt/blob-storage/container2/directory2]=fileNotification2-out-0
```

## Kafka Topics

- **File Notifications**: `kafka.topic1`, `kafka.topic2` (configurable per directory)
- **Error Channel**: `kafka.error.topic` (all processing errors)

## Modern Java Features Used

- **var keyword**: Type inference for improved readability
- **Records**: Immutable data classes for events
- **Switch expressions**: Pattern matching in utility methods
- **Stream API**: Functional processing of files and directories
- **Immutable collections**: Map.of() for better functional programming
- **Try-with-resources**: Automatic resource management

## Adapting for Kafka Consumer Pattern

The service can be easily adapted to consume file processing messages from Kafka:

1. Uncomment the example consumer in `KafkaConsumerConfig`
2. Configure the input binding in `application.properties`
3. The existing `FileProcessor` can be reused for message-driven file processing

See `KafkaConsumerConfig.java` for details.

## Error Handling

The service implements comprehensive error handling:

- Retry logic for transient failures (IOException, FileSystemException)
- Error notification to dedicated Kafka error channel
- Logging at appropriate levels (info, error, debug)
- Graceful fallback for blob storage limitations (e.g., atomic move not supported)

## Building and Running

```bash
# Build
./gradlew build

# Run
./gradlew bootRun

# Run tests
./gradlew test
```

## Best Practices Implemented

- **Immutability**: Records for events, Map.of() for metadata
- **Functional Programming**: Stream API, method references, predicates
- **Clean Code**: JavaDoc, clear naming, single responsibility
- **Configuration**: Externalized, type-safe with @ConfigurationProperties
- **Observability**: Structured logging, error channels
- **Resilience**: Retry logic, error handling, fallback mechanisms
