package com.agilab.file_loading.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Event representing a successfully processed file.
 * Part of the sealed FileEvent hierarchy.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public final class FileProcessedEvent implements FileEvent {
    private String originalFilePath;
    private String processedFilePath;
    private String sourceDirectory;  // Changed to sourceDirectory for producer to resolve binding
    private Instant timestamp;
    private String fileName;
    @Builder.Default
    private Map<String, Object> metadata = new HashMap<>();
}