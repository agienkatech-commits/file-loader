package com.agilab.file_loading.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.support.RetryTemplate;

import java.io.IOException;
import java.nio.file.FileSystemException;
import java.util.List;

@Configuration
public class FileLoaderBeans {

    @Bean
    public RetryTemplate retryTemplate(FileLoaderProperties properties) {
        return RetryTemplate.builder()
                .maxAttempts(properties.getRetryAttempts())
                .fixedBackoff(properties.getRetryDelay().toMillis())
                .retryOn(List.of(IOException.class, FileSystemException.class))
                .build();
    }
}
