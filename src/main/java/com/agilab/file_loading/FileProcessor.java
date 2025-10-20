package com.agilab.file_loading;

import com.agilab.file_loading.config.FileLoaderProperties;
import com.agilab.file_loading.event.FileProcessedEvent;
import com.agilab.file_loading.util.FilesHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class FileProcessor {

    private final RetryTemplate retryTemplate;
    private final FileLoaderProperties properties;
    private final FilesHelper filesHelper;

    Optional<FileProcessedEvent> processFileWithRetry(Path sourceFile, String baseDirectory) {
        try {
            return retryTemplate.execute(context -> processFile(sourceFile, baseDirectory));
        } catch (Exception e) {
            log.error("Failed to process file after {} attempts: {}",
                    properties.getRetryAttempts(), sourceFile, e);
            // Move to an error directory
            handleFailedFile(sourceFile, baseDirectory);
            return Optional.empty();
        }
    }

    private Optional<FileProcessedEvent> processFile(Path sourceFile, String baseDirectory) {
        try {
            // 1. Read attributes from the source file first
            var metadata = filesHelper.buildFileMetadata(sourceFile);

            // 2. Ensure processed directory exists
            var processedDir = Paths.get(baseDirectory, properties.getProcessedSubdirectory());
            Files.createDirectories(processedDir);

            // 3. Generate timestamp and new filename
            var originalName = sourceFile.getFileName().toString();
            var newFileName = getNewFileName(originalName);
            var targetFile = processedDir.resolve(newFileName);

            // 4. Move the file
            filesHelper.moveFileAtomically(sourceFile, targetFile);

            log.info("File processed: {} -> {}", sourceFile, targetFile);

            // 5. Update metadata with processed file info
            filesHelper.updateMetadataWithTarget(metadata, targetFile);

            return Optional.of(FileProcessedEvent.builder()
                    .originalFilePath(sourceFile.toString())
                    .processedFilePath(targetFile.toString())
                    .sourceDirectory(baseDirectory)
                    .timestamp(Instant.now())
                    .fileName(originalName)
                    .metadata(metadata) // Use the populated metadata map
                    .build());

        } catch (Exception e) {
            log.error("Error processing file: {}", sourceFile, e);
            throw new RuntimeException("File processing failed", e);
        }
    }

    private String getNewFileName(String originalName) {
        var timestamp = Instant.now().toString().replaceAll(":", "-"); // Azure-friendly timestamp
        var nameWithoutExtension = filesHelper.getNameWithoutExtension(originalName);
        var extension = filesHelper.getFileExtension(originalName);
        return String.format("%s-%s%s", nameWithoutExtension, timestamp, extension);
    }

    private void handleFailedFile(Path sourceFile, String baseDirectory) {
        try {
            var errorDir = Paths.get(baseDirectory, properties.getErrorSubdirectory());
            Files.createDirectories(errorDir);
            var targetFile = errorDir.resolve(sourceFile.getFileName());
            filesHelper.moveFileAtomically(sourceFile, targetFile);
            log.info("Moved failed file to error directory: {}", targetFile);
            // Optionally, send a notification to a DLQ topic
        } catch (IOException ex) {
            log.error("Could not move failed file to error directory: {}", sourceFile, ex);
        }
    }
}
