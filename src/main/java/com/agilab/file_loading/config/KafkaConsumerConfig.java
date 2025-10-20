package com.agilab.file_loading.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

/**
 * Configuration for Kafka consumer integration.
 * This demonstrates how to adapt this service for a Kafka-consumer to file-processor pattern.
 * 
 * To enable Kafka consumption:
 * 1. Define a consumer function bean
 * 2. Configure the binding in application.properties
 * 3. The FileProcessor can be reused to process files referenced in Kafka messages
 */
@Configuration
@Slf4j
public class KafkaConsumerConfig {

    /**
     * Example consumer for file processing messages from Kafka.
     * Uncomment and adapt as needed for your specific use case.
     */
    /*
    @Bean
    public Consumer<FileProcessingMessage> processFileMessage(FileProcessor fileProcessor) {
        return message -> {
            try {
                log.info("Received file processing message: {}", message);
                var filePath = Paths.get(message.filePath());
                fileProcessor.processFile(filePath, message.baseDirectory());
            } catch (Exception e) {
                log.error("Error processing Kafka message: {}", message, e);
                // Error will be sent to error channel by FileProcessor
            }
        };
    }
    */

    /**
     * Example message record for Kafka file processing.
     * Uncomment and adapt as needed.
     */
    /*
    public record FileProcessingMessage(
            String filePath,
            String baseDirectory,
            Instant timestamp,
            Map<String, Object> metadata
    ) {}
    */
}
