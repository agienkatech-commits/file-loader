package com.agilab.file_loading;

import com.agilab.file_loading.config.FileLoaderProperties;
import com.agilab.file_loading.event.FileLoadedEvent;
import com.agilab.file_loading.notification.FileNotificationProducer;
import com.agilab.file_loading.util.FileOperations;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Map;

import static org.apache.commons.io.FilenameUtils.getBaseName;

@Service
@Slf4j
@RequiredArgsConstructor

public class ScheduledFileCleaner {

    private final FileLoaderProperties properties;
    private final FileNotificationProducer notificationProducer;
    private final FileOperations fileOperations;

//    @Scheduled(fixedRateString = "#{@fileLoaderProperties.cleaningInterval.toMillis()}")
    public void cleanStickFiles() {
        properties.getSourceDirectories().keySet().parallelStream()
                .forEach(this::cleanLoadingDirectory);
    }

    private void cleanLoadingDirectory(String baseDirectory) {
        try {
            var loadingDir = Paths.get(baseDirectory, properties.getLoadingSubdirectory());
            var loadedDir = Paths.get(baseDirectory, properties.getLoadedSubdirectory());

            if (!Files.exists(loadingDir)) {
                log.debug("Loading dir does not exist: {}", loadingDir);
                return;
            }

            Files.list(loadingDir)
                    .filter(Files::isRegularFile)
                    .filter(this::isOldEnough)
                    .forEach(file -> sendNotificationAndMoveToLoaded(file, loadedDir, baseDirectory));

        } catch (Exception e) {
            log.error("Error cleaning loading directory: {}", baseDirectory, e);
        }
    }

    private boolean isOldEnough(Path file) {
        var cutoffTime = Instant.now().minus(properties.getStuckFileThreshold());
        try {
            return Files.getLastModifiedTime(file).toInstant().isBefore(cutoffTime);
        } catch (IOException e) {
            log.warn("Cannot read file last modified time: {}", file);
            return false;
        }
    }

    @SneakyThrows
    private void sendNotificationAndMoveToLoaded(Path file, Path loadedDir, String baseDirectory) {
        log.warn("The file {} was not processed normally. Cleaning process is about to resend the notification.", file);
        var fineName = getBaseName(file.toString());
        var loadedFile = loadedDir.resolve(fineName);
        Files.createDirectories(loadedDir);
        var fileLoadedEvent = new FileLoadedEvent(file.toString(), loadedFile.toString(), baseDirectory, Instant.now(), fineName, Map.of());
        var notificationSent = notificationProducer.sendFileNotification(fileLoadedEvent);
        if (notificationSent) {
            fileOperations.moveFileAtomicallyWithRetry(file, loadedFile);
            log.info("A file notification resent successfully: {}", loadedFile);
        }
    }
}