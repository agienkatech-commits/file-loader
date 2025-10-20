package com.agilab.file_loading.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Event representing a file processing error.
 * Part of the sealed FileEvent hierarchy.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public final class FileErrorEvent implements FileEvent {
    private String originalFilePath;
    private String sourceDirectory;
    private Instant timestamp;
    private String fileName;
    private String errorMessage;
    private String errorType;
}
