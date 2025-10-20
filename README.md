# File Loader Microservice

A Spring Boot 3.5.6 microservice built with Java 25, demonstrating modern Java features for file processing and event streaming.

## Features

- 🔄 **Automated File Processing**: Polls configured directories for new files and processes them
- 📤 **Event Streaming**: Publishes file processing events to Kafka topics via Spring Cloud Stream
- 🔁 **Retry Logic**: Automatic retry with configurable attempts and delays
- 🎯 **Type Safety**: Sealed classes for events and exceptions with pattern matching
- ⚡ **Modern Java**: Leverages Java 25 features (var, enhanced switch, pattern matching)
- 🧪 **Well Tested**: Comprehensive test suite with 19 passing tests

## Java 25 Features Demonstrated

### Sealed Classes
```java
// Event hierarchy
public sealed interface FileEvent 
        permits FileProcessedEvent, FileErrorEvent { }

// Exception hierarchy  
public sealed interface FileProcessingException 
        permits FileReadException, FileWriteException, FileNotFoundException { }
```

### Pattern Matching with Switch
```java
public String getEventDescription(FileEvent event) {
    return switch (event) {
        case FileProcessedEvent processed -> "Success: " + processed.getFileName();
        case FileErrorEvent error -> "Error: " + error.getErrorMessage();
    };
}
```

### Enhanced Switch Expressions
```java
public String getFileExtension(String fileName) {
    var lastDotIndex = fileName.lastIndexOf('.');
    return switch (lastDotIndex) {
        case -1, 0 -> "";
        default -> fileName.substring(lastDotIndex);
    };
}
```

### var Keyword
Used throughout for local variables with inferred types.

## Prerequisites

- Java 25 (with preview features enabled)
- Gradle 8.14.3+ (included via wrapper)
- Kafka (for Spring Cloud Stream)

## Configuration

### application.properties

```properties
# Application name
spring.application.name=file-loading

# Kafka configuration
spring.cloud.stream.kafka.binder.brokers=localhost:9092

# File loader configuration
file-loader.source-directories./mnt/blob-storage/container1/directory1=fileNotification1-out-0
file-loader.source-directories./mnt/blob-storage/container2/directory2=fileNotification2-out-0
file-loader.polling-interval=PT3S
file-loader.batch-size=50
file-loader.retry-attempts=3
file-loader.retry-delay=PT2S
```

### Directory Structure

Each configured source directory should have these subdirectories:
```
/mnt/blob-storage/container1/directory1/
├── new/          # Place new files here
├── processed/    # Successfully processed files
└── error/        # Failed files
```

## Building

```bash
# Clean build
./gradlew clean build

# Run tests
./gradlew test

# Build without tests
./gradlew build -x test
```

## Running

### Start Kafka (required)
```bash
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_ENABLE_KRAFT=yes \
  apache/kafka:latest
```

### Run the application
```bash
# Using Gradle
./gradlew bootRun

# Using JAR
java --enable-preview -jar build/libs/file-loading-0.0.1-SNAPSHOT.jar
```

## How It Works

1. **Polling**: The scheduler polls configured directories every 3 seconds
2. **File Detection**: Finds stable files (not being written) in the `new/` subdirectory
3. **Processing**: 
   - Reads file metadata
   - Moves file to `processed/` with timestamp
   - Creates FileProcessedEvent
4. **Event Publishing**: Sends events to configured Kafka topics
5. **Error Handling**: Failed files moved to `error/` directory

## Project Structure

```
src/main/java/com/agilab/file_loading/
├── BlobFilePollingScheduler.java       # Scheduled file polling
├── FileLoadingApplication.java         # Spring Boot main class
├── FileProcessingService.java          # Orchestrates file processing
├── FileProcessor.java                  # Handles individual file processing
├── config/
│   ├── FileLoaderBeans.java           # Bean configurations
│   └── FileLoaderProperties.java      # Configuration properties
├── event/
│   ├── FileEvent.java                 # Sealed event interface
│   ├── FileProcessedEvent.java        # Success event
│   └── FileErrorEvent.java            # Error event
├── exception/
│   ├── FileProcessingException.java   # Sealed exception interface
│   ├── FileReadException.java         # Read error
│   ├── FileWriteException.java        # Write error
│   ├── FileNotFoundException.java     # Not found error
│   └── FileExceptionHandler.java      # Exception handler with pattern matching
├── notification/
│   └── FileNotificationProducer.java  # Kafka event producer
└── util/
    ├── FilesHelper.java               # File utility methods
    └── FileEventHandler.java          # Event handler with pattern matching
```

## Testing

Run all tests:
```bash
./gradlew test
```

Test classes:
- `FilesHelperTest` - File name parsing and filtering (8 tests)
- `FileEventTest` - Sealed event types and pattern matching (5 tests)
- `FileProcessingExceptionTest` - Sealed exceptions and error handling (6 tests)

## Performance Considerations

### Blocking Operations
The `FilesHelper.isFileStable()` method uses `Thread.sleep()` to verify file stability. For high-throughput scenarios, consider:

- **File System Watchers**: Use `WatchService` for event-driven processing
- **Async Processing**: Use Spring's `@Async` with `CompletableFuture`
- **Reactive Streams**: Consider Project Reactor for non-blocking operations

### Batch Processing
Configure `file-loader.batch-size` to control memory usage and processing throughput.

## Monitoring

### Actuator Endpoints
The application includes Spring Boot Actuator:
- Health: `http://localhost:8080/actuator/health`
- Info: `http://localhost:8080/actuator/info`

### Logging
Configured loggers:
- `com.agilab.file_loading.FileProcessingService` - File processing logs
- `com.agilab.file_loading.BlobFilePollingScheduler` - Polling logs

## Documentation

- [JAVA25_IMPROVEMENTS.md](JAVA25_IMPROVEMENTS.md) - Detailed improvements guide
- [CODE_REVIEW_SUMMARY.md](CODE_REVIEW_SUMMARY.md) - Executive code review summary

## Development

### IDE Setup

**IntelliJ IDEA**:
1. File → Project Structure → Project SDK: Java 25
2. Settings → Build → Compiler → Java Compiler: Release 25
3. Enable preview features in compiler settings

**VS Code**:
1. Install Java Extension Pack
2. Configure `java.configuration.runtimes` to use Java 25
3. Enable preview features in settings

### Adding New File Sources

1. Add directory mapping in `application.properties`:
```properties
file-loader.source-directories./your/new/path=yourTopic-out-0
```

2. Configure Spring Cloud Stream binding:
```properties
spring.cloud.stream.bindings.yourTopic-out-0.destination=kafka.your.topic
spring.cloud.stream.bindings.yourTopic-out-0.content-type=application/json
```

## Contributing

1. Follow the existing code style (using var, sealed types, pattern matching)
2. Add tests for new features
3. Update documentation
4. Ensure `./gradlew build` passes

## License

This is a demo project for Java 25 features demonstration.

## Support

For questions or issues, refer to:
- [Spring Boot Documentation](https://spring.io/projects/spring-boot)
- [Spring Cloud Stream Documentation](https://spring.io/projects/spring-cloud-stream)
- [Java 25 Release Notes](https://openjdk.org/projects/jdk/25/)
