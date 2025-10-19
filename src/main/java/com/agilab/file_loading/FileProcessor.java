package com.agilab.file_loading;

import com.agilab.file_loading.config.FileLoaderProperties;
import com.agilab.file_loading.event.FileProcessedEvent;
import com.agilab.file_loading.util.FilesHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

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
            return Optional.empty();
        }
    }

    private Optional<FileProcessedEvent> processFile(Path sourceFile, String baseDirectory) {
        try {
            // Ensure processed directory exists
            var processedDir = Paths.get(baseDirectory, properties.getProcessedSubdirectory());
            Files.createDirectories(processedDir);

            // Generate timestamp and new filename
            var originalName = sourceFile.getFileName().toString();
            var newFileName = getNewFileName(originalName);
            var targetFile = processedDir.resolve(newFileName);

            // For Azure Blob Storage mounted volumes, use atomic move operation
            filesHelper.moveFileAtomically(sourceFile, targetFile);

            log.info("File processed: {} -> {}", sourceFile, targetFile);

            return Optional.of(FileProcessedEvent.builder()
                    .originalFilePath(sourceFile.toString())
                    .processedFilePath(targetFile.toString())
                    .sourceDirectory(baseDirectory)
                    .timestamp(Instant.now())
                    .fileName(originalName)
                    .baseDirectory(baseDirectory)
                    .metadata(filesHelper.buildFileMetadata(sourceFile, targetFile))
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
}
