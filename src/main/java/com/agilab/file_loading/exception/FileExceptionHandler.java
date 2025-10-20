package com.agilab.file_loading.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Handler for file processing exceptions using Java 25 pattern matching.
 */
@Slf4j
@Component
public class FileExceptionHandler {

    /**
     * Gets error type from exception using pattern matching.
     * Demonstrates Java 25's enhanced switch with sealed types.
     */
    public String getErrorType(FileProcessingException exception) {
        return switch (exception) {
            case FileReadException e -> "READ_ERROR";
            case FileWriteException e -> "WRITE_ERROR";
            case FileNotFoundException e -> "NOT_FOUND";
        };
    }

    /**
     * Gets error severity based on exception type.
     */
    public String getErrorSeverity(FileProcessingException exception) {
        return switch (exception) {
            case FileReadException e -> "MEDIUM";
            case FileWriteException e -> "HIGH";
            case FileNotFoundException e -> "LOW";
        };
    }

    /**
     * Logs exception with appropriate level.
     */
    public void logException(FileProcessingException exception) {
        var errorType = getErrorType(exception);
        var severity = getErrorSeverity(exception);
        var filePath = exception.getFilePath();
        
        switch (exception) {
            case FileReadException e -> 
                    log.warn("[{}] Failed to read file: {} - {}", severity, filePath, e.getMessage());
            case FileWriteException e -> 
                    log.error("[{}] Failed to write file: {} - {}", severity, filePath, e.getMessage());
            case FileNotFoundException e -> 
                    log.info("[{}] File not found: {} - {}", severity, filePath, e.getMessage());
        }
    }
}
