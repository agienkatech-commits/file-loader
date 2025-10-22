package com.agilab.file_loading;

import com.agilab.file_loading.config.FileLoaderProperties;
import com.agilab.file_loading.event.FileLoadedEvent;
import com.agilab.file_loading.notification.FileNotificationProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Map;

import static com.agilab.file_loading.util.FilesHelper.moveFileAtomically;
import static org.apache.commons.io.FilenameUtils.getBaseName;

@Service
@Slf4j
@RequiredArgsConstructor
public class ScheduledFileCleaner {

    private final FileLoaderProperties properties;
    private final FileNotificationProducer notificationProducer;
    private final RetryTemplate retryTemplate;

    @Scheduled(fixedRateString = "#{@fileLoaderProperties.cleaningInterval.toMillis()}")
    public void cleanProcessedFiles() {
        properties.getSourceDirectories().keySet()
                .forEach(this::cleanLoadingDirectory);
    }

    private void cleanLoadingDirectory(String baseDirectory) {
        try {
            var loadingDir = Paths.get(baseDirectory, properties.getLoadingSubdirectory());
            var loadedDir = Paths.get(baseDirectory, properties.getLoadedSubdirectory());

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

    private void sendNotificationAndMoveToLoaded(Path file, Path loadedDir, String baseDirectory) {
        log.warn("The file {} was not processed normally. Cleaning process is about to resend the notification.", file);
        var fineName = getBaseName(file.toString());
        var loadedFile = loadedDir.resolve(fineName);
        var fileLoadedEvent = new FileLoadedEvent(file.toString(), loadedFile.toString(), baseDirectory, Instant.now(), fineName, Map.of());
        var notificationSent = notificationProducer.sendFileNotification(fileLoadedEvent);
        if (notificationSent) {
            retryTemplate.execute(context -> moveFileAtomically(file, loadedFile));
            log.info("A file notification resent successfully: {}", loadedFile);
        }
    }
}