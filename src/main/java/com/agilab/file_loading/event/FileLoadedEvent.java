package com.agilab.file_loading.event;

import java.time.Instant;
import java.util.Map;

public record FileLoadedEvent(String originalFilePath,
                              String loadedFilePath,
                              String baseDirectory,
                              Instant timestamp,
                              String fileName,
                              Map<String, Object> metadata) {
}