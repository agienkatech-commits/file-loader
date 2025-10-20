package com.agilab.file_loading;

import com.agilab.file_loading.config.FileLoaderProperties;
import java.nio.file.Paths;
import com.agilab.file_loading.event.FileProcessingErrorEvent;
import com.agilab.file_loading.notification.ErrorNotificationProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;

import static com.agilab.file_loading.util.FilesHelper.findNewFiles;

/**
 * Service responsible for processing new files from configured directories.
 * Scans directories in parallel and delegates individual file processing to FileProcessor.
 * Implements error handling with notification to error channel.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class FileProcessingService {

    private final FileLoaderProperties properties;
    private final FileProcessor fileProcessor;
    private final ErrorNotificationProducer errorNotificationProducer;

    public void processNewFiles() {
        properties.getSourceDirectories().keySet().stream()
                .parallel()
                .forEach(this::processDirectory);
    }

    private void processDirectory(String baseDirectory) {
        try {
            var newFilesPath = Paths.get(baseDirectory, properties.getNewSubdirectory());
            findNewFiles(newFilesPath, properties.getFileStabilityCheckDelay())
                    .forEach(file -> fileProcessor.processFile(file, baseDirectory));

        } catch (Exception e) {
            log.error("Error processing directory: {}", baseDirectory, e);
            sendErrorNotification(baseDirectory, null, e);
        }
    }

    private void sendErrorNotification(String baseDirectory, String filePath, Exception e) {
        var errorEvent = new FileProcessingErrorEvent(
                filePath != null ? filePath : baseDirectory,
                baseDirectory,
                e.getMessage(),
                e.getClass().getSimpleName(),
                Instant.now(),
                Map.of()
        );
        errorNotificationProducer.sendErrorNotification(errorEvent);
    }
}