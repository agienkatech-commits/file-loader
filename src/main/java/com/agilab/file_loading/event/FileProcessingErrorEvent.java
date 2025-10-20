package com.agilab.file_loading.event;

import java.time.Instant;
import java.util.Map;

public record FileProcessingErrorEvent(String filePath,
                                       String baseDirectory,
                                       String errorMessage,
                                       String errorType,
                                       Instant timestamp,
                                       Map<String, Object> metadata) {
}
