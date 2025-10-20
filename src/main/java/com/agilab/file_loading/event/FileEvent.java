package com.agilab.file_loading.event;

import java.time.Instant;

/**
 * Sealed interface representing file processing events.
 * Uses Java 25 sealed types to restrict the event hierarchy.
 */
public sealed interface FileEvent permits FileProcessedEvent, FileErrorEvent {
    String getFileName();
    Instant getTimestamp();
    String getSourceDirectory();
}
