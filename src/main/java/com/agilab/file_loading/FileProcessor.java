package com.agilab.file_loading;

import com.agilab.file_loading.config.FileLoaderProperties;
import com.agilab.file_loading.event.FileLoadedEvent;
import com.agilab.file_loading.notification.FileNotificationProducer;
import com.agilab.file_loading.util.FileOperations;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;

import static com.agilab.file_loading.util.FileOperations.getFileExtension;
import static com.agilab.file_loading.util.FileOperations.getNameWithoutExtension;

@Slf4j
@Component
@RequiredArgsConstructor
public class FileProcessor {

    private final FileNotificationProducer notificationProducer;
    private final FileOperations fileOperations;
    private final FileLoaderProperties properties;

    void processFile(Path sourceFile, String baseDirectory) {
        try {
            var loadedDir = Paths.get(baseDirectory, properties.getLoadedSubdirectory());
            var loadedFileName = getNewFileName(sourceFile.getFileName().toString());
            var loadedFilePath = loadedDir.resolve(loadedFileName);
            fileOperations.moveFileAtomicallyWithRetry(sourceFile, loadedFilePath);
            var loadedEvent = createLoadedEvent(sourceFile, baseDirectory, loadedFilePath, loadedFileName);
            var notificationSent = notificationProducer.sendFileNotification(loadedEvent);
            if (notificationSent) {
                log.info("Successfully moved to: {} and sent notification", loadedFilePath);
            }
        } catch (Exception e) {
            log.error("Failed to process file: {}", sourceFile, e);
        }
    }

    private FileLoadedEvent createLoadedEvent(Path sourceFile, String baseDirectory, Path loadedFile, String newFileName) {
        return new FileLoadedEvent(sourceFile.toString(),loadedFile.toString(), baseDirectory, Instant.now(), newFileName, new HashMap<>());
    }

    private String getNewFileName(String originalName) {
        var timestamp = Instant.now().toString().replaceAll(":", "-"); // Azure-friendly timestamp
        var nameWithoutExtension = getNameWithoutExtension(originalName);
        var extension = getFileExtension(originalName);
        return String.format("%s-%s%s", nameWithoutExtension, timestamp, extension);
    }
}
