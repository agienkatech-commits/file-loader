package com.agilab.file_loading.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.support.RetryTemplate;

import java.io.IOException;
import java.nio.file.FileSystemException;
import java.util.List;

/**
 * Bean configuration for file loader components.
 * Configures retry template for file operations with appropriate exception handling.
 */
@Configuration
public class FileLoaderBeans {

    /**
     * RetryTemplate for file operations.
     * Retries on IOException and FileSystemException which are common in blob storage.
     */
    @Bean
    public RetryTemplate retryTemplate(FileLoaderProperties properties) {
        return RetryTemplate.builder()
                .maxAttempts(properties.getRetryAttempts())
                .fixedBackoff(properties.getRetryDelay().toMillis())
                .retryOn(List.of(IOException.class, FileSystemException.class))
                .build();
    }
}
