package com.agilab.file_loading.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;

/**
 * Configuration properties for the file loader microservice.
 * Binds to "file-loader" prefix in application.properties.
 * Uses immutable defaults where possible for better functional programming style.
 */
@ConfigurationProperties(prefix = "file-loader")
@Data
@Component
public class FileLoaderProperties {
    private Map<String, String> sourceDirectories = Map.of();
    private Duration pollingInterval = Duration.ofSeconds(3);
    private String newSubdirectory = "new";
    private String loadingSubdirectory = "loading";
    private String loadedSubdirectory = "loaded";
    private String errorSubdirectory = "error";
    private int retryAttempts = 3;
    private Duration retryDelay = Duration.ofSeconds(2);
    private Duration fileStabilityCheckDelay = Duration.ofSeconds(1);
    private Duration stuckFileThreshold = Duration.ofMinutes(5);
}
