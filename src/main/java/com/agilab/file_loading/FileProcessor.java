package com.agilab.file_loading;

import com.agilab.file_loading.config.FileLoaderProperties;
import com.agilab.file_loading.event.FileProcessedEvent;
import com.agilab.file_loading.notification.FileNotificationProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;

import static com.agilab.file_loading.util.FilesHelper.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class FileProcessor {

    private final FileNotificationProducer notificationProducer;
    private final RetryTemplate retryTemplate;
    private final FileLoaderProperties properties;

    void processFile(Path sourceFile, String baseDirectory) {
        try {
            var loadingDir = Paths.get(baseDirectory, properties.getLoadingSubdirectory());
            var loadingFile = loadingDir.resolve(sourceFile.getFileName());
            var movedPath = retryTemplate.execute(context -> moveFileAtomically(sourceFile, loadingFile));
            log.info("Moved to processing: {}", movedPath);

            var loadedDir = Paths.get(baseDirectory, properties.getLoadedSubdirectory());
            var loadedFileName = getNewFileName(sourceFile.getFileName().toString());
            var loadedFilePath = loadedDir.resolve(loadedFileName);
            var loadedEvent = createLoadedEvent(sourceFile, baseDirectory, loadedFilePath, loadedFileName);
            var notificationSent = notificationProducer.sendFileNotification(loadedEvent);

            if (notificationSent) {
                retryTemplate.execute(context -> moveFileAtomically(loadingFile, loadedFilePath));
                log.info("Successfully processed and moved to: {}", loadedFilePath);
            }
        } catch (Exception e) {
            log.error("Failed to process file after {} attempts: {}",
                    properties.getRetryAttempts(), sourceFile, e);
        }
    }

    private FileProcessedEvent createLoadedEvent(Path sourceFile, String baseDirectory, Path loadedFile, String newFileName) {
        return new FileProcessedEvent(sourceFile.toString(),loadedFile.toString(), baseDirectory, Instant.now(), newFileName, new HashMap<>());
    }

    private String getNewFileName(String originalName) {
        var timestamp = Instant.now().toString().replaceAll(":", "-"); // Azure-friendly timestamp
        var nameWithoutExtension = getNameWithoutExtension(originalName);
        var extension = getFileExtension(originalName);
        return String.format("%s-%s%s", nameWithoutExtension, timestamp, extension);
    }
}
