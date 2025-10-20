package com.agilab.file_loading.util;

import com.agilab.file_loading.event.FileErrorEvent;
import com.agilab.file_loading.event.FileEvent;
import com.agilab.file_loading.event.FileProcessedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Utility for handling file events using Java 25 pattern matching.
 */
@Slf4j
@Component
public class FileEventHandler {

    /**
     * Processes a file event using pattern matching with sealed types.
     * Demonstrates Java 25's enhanced switch with pattern matching.
     */
    public String getEventDescription(FileEvent event) {
        return switch (event) {
            case FileProcessedEvent processed ->
                    String.format("Successfully processed file '%s' from %s to %s",
                            processed.getFileName(),
                            processed.getOriginalFilePath(),
                            processed.getProcessedFilePath());
            case FileErrorEvent error ->
                    String.format("Failed to process file '%s': %s (%s)",
                            error.getFileName(),
                            error.getErrorMessage(),
                            error.getErrorType());
        };
    }

    /**
     * Logs event with appropriate level based on event type.
     */
    public void logEvent(FileEvent event) {
        switch (event) {
            case FileProcessedEvent processed -> 
                    log.info("Processed: {}", getEventDescription(processed));
            case FileErrorEvent error -> 
                    log.error("Error: {}", getEventDescription(error));
        }
    }
}
