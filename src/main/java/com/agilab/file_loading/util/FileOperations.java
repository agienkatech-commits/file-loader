package com.agilab.file_loading.util;

import com.agilab.file_loading.config.FileLoaderProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@RequiredArgsConstructor
@Component
public class FileOperations {

    private final RetryTemplate retryTemplate;
    private final FileLoaderProperties properties;

    public static List<Path> findNewFiles(Path newDirectory) throws IOException {
        try (Stream<Path> files = Files.list(newDirectory)) {
            return files
                    .filter(Files::isRegularFile)
                    .filter(FileOperations::isFileStable) // Check if file is fully written
                    .filter(FileOperations::isNotTemporaryFile)
                    .sorted(Comparator.comparing(path -> {
                        try {
                            return Files.getLastModifiedTime(path);
                        } catch (IOException e) {
                            return FileTime.fromMillis(0);
                        }
                    }))
                    .collect(Collectors.toList());
        }
    }

    private static boolean isNotTemporaryFile(Path file) {
        var fileName = file.getFileName().toString();
        return !fileName.startsWith(".")
                && !fileName.startsWith("~")
                && !fileName.endsWith(".tmp");
    }

    private static boolean isFileStable(Path file) {
        try {
            // Check if file size is stable (not being written to)
            long size1 = Files.size(file);
            Thread.sleep(1000);
            long size2 = Files.size(file);

            return size1 == size2 && size1 > 0;
        } catch (Exception e) {
            log.debug("File stability check failed for: {}", file, e);
            return false;
        }
    }

    public void moveFileAtomicallyWithRetry(Path source, Path target) {
        try {
            retryTemplate.execute(context -> moveFileAtomically(source, target));
        } catch (IOException e) {
            log.error("Failed to process file after {} attempts: {}",
                    properties.getRetryAttempts(), source, e);
        }
    }

    public static Path moveFileAtomically(Path source, Path target) throws IOException {
        try {
            // Try atomic move first (works on most mounted volumes)
            return Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            log.debug("Atomic move not supported, falling back to copy+delete for: {}", source);
            // Fallback: copy then delete (for blob storage that doesn't support atomic moves)
            var targetPath = Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
            Files.delete(source);
            return targetPath;
        }
    }

    public static String getNameWithoutExtension(String fileName) {
        var lastDotIndex = fileName.lastIndexOf('.');
        return switch (lastDotIndex) {
            case -1, 0 -> fileName;
            default -> fileName.substring(0, lastDotIndex);
        };
    }

    public static String getFileExtension(String fileName) {
        var lastDotIndex = fileName.lastIndexOf('.');
        return switch (lastDotIndex) {
            case -1, 0 -> "";
            default -> fileName.substring(lastDotIndex);
        };
    }
}
