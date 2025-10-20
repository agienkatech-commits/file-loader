package com.agilab.file_loading.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FileProcessedEvent {
    private String originalFilePath;
    private String processedFilePath;
    private String sourceDirectory;  // Changed to sourceDirectory for producer to resolve binding
    private Instant timestamp;
    private String fileName;
    private Map<String, Object> metadata = new HashMap<>();
}