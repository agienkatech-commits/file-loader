package com.agilab.file_loading.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public record FileProcessedEvent (String originalFilePath,
                                  String processedFilePath,
                                  String sourceDirectory,
                                  Instant timestamp,
                                  String fileName,
                                  Map<String, Object> metadata) {
}