package com.agilab.file_loading.util;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.*;
import java.util.function.Predicate;

/**
 * Utility class for file operations with focus on robustness and blob storage compatibility.
 * Uses modern Java features including Stream API, var keyword, and functional programming patterns.
 */
@Slf4j
@RequiredArgsConstructor
public class FilesHelper {

    /**
     * Find new files in a directory that are ready for processing.
     * Filters out temporary files and checks file stability.
     *
     * @param newDirectory          directory to scan for files
     * @param stabilityCheckDelay   delay to use when checking if file is stable
     * @return list of stable, non-temporary files sorted by last modified time
     * @throws IOException if directory cannot be read
     */
    public static List<Path> findNewFiles(Path newDirectory, Duration stabilityCheckDelay) throws IOException {
        try (var files = Files.list(newDirectory)) {
            return files
                    .filter(Files::isRegularFile)
                    .filter(file -> isFileStable(file, stabilityCheckDelay))
                    .filter(isNotTemporaryFile())
                    .sorted(FilesHelper::compareByLastModified)
                    .toList();
        }
    }

    /**
     * Predicate to filter out temporary files.
     * Temporary files start with "." or "~" or end with ".tmp"
     */
    private static Predicate<Path> isNotTemporaryFile() {
        return file -> {
            var fileName = file.getFileName().toString();
            return !fileName.startsWith(".")
                    && !fileName.startsWith("~")
                    && !fileName.endsWith(".tmp");
        };
    }

    /**
     * Compare files by last modified time for sorting.
     */
    private static int compareByLastModified(Path p1, Path p2) {
        try {
            return Files.getLastModifiedTime(p1).compareTo(Files.getLastModifiedTime(p2));
        } catch (IOException e) {
            log.debug("Error comparing file times: {} and {}", p1, p2, e);
            return 0;
        }
    }

    /**
     * Check if a file is stable (not being written to).
     * Compares file size before and after a delay to detect ongoing writes.
     *
     * @param file                  file to check
     * @param stabilityCheckDelay   how long to wait between size checks
     * @return true if file size is stable and greater than zero
     */
    private static boolean isFileStable(Path file, Duration stabilityCheckDelay) {
        try {
            var size1 = Files.size(file);
            Thread.sleep(stabilityCheckDelay.toMillis());
            var size2 = Files.size(file);

            return size1 == size2 && size1 > 0;
        } catch (Exception e) {
            log.debug("File stability check failed for: {}", file, e);
            return false;
        }
    }

    /**
     * Move file atomically with fallback for storage that doesn't support atomic moves.
     * This is important for blob storage mounted as file systems.
     *
     * @param source  source file path
     * @param target  target file path
     * @return the target path
     * @throws IOException if move fails
     */
    @SneakyThrows
    public static Path moveFileAtomically(Path source, Path target) {
        try {
            return Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            log.debug("Atomic move not supported, falling back to copy+delete for: {}", source);
            var targetPath = Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
            Files.delete(source);
            return targetPath;
        }
    }

    /**
     * Extract filename without extension.
     * Uses switch expression (Java 14+).
     */
    public static String getNameWithoutExtension(String fileName) {
        var lastDotIndex = fileName.lastIndexOf('.');
        return switch (lastDotIndex) {
            case -1, 0 -> fileName;
            default -> fileName.substring(0, lastDotIndex);
        };
    }

    /**
     * Extract file extension including the dot.
     * Uses switch expression (Java 14+).
     */
    public static String getFileExtension(String fileName) {
        var lastDotIndex = fileName.lastIndexOf('.');
        return switch (lastDotIndex) {
            case -1, 0 -> "";
            default -> fileName.substring(lastDotIndex);
        };
    }
}
