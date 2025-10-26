package com.agilab.file_loading.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "file-loader")
@Data
@Component
public class FileLoaderProperties {
    private Map<String, String> sourceDirectories = new HashMap<>();
    private Duration pollingInterval = Duration.ofSeconds(3); // Slightly longer for blob storage
    private Duration cleaningInterval = Duration.ofMinutes(5); // Slightly longer for blob storage
    private String newSubdirectory = "flow1/new";
    private String loadingSubdirectory = "loading";
    private String loadedSubdirectory = "flow1/loaded";
    private int retryAttempts = 3;
    private Duration retryDelay = Duration.ofSeconds(2);
    private Duration stuckFileThreshold = Duration.ofMinutes(1);
}
