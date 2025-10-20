package com.agilab.file_loading.util;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@RequiredArgsConstructor
public class FilesHelper {

    public static List<Path> findNewFiles(Path newDirectory) throws IOException {
        try (Stream<Path> files = Files.list(newDirectory)) {
            return files
                    .filter(Files::isRegularFile)
                    .filter(FilesHelper::isFileStable) // Check if file is fully written
                    .filter(file -> !file.getFileName().toString().startsWith("."))
                    .filter(file -> !file.getFileName().toString().startsWith("~"))
                    .filter(file -> !file.getFileName().toString().endsWith(".tmp"))
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

    @SneakyThrows
    public static Path moveFileAtomically(Path source, Path target) {
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
        return Optional.of(fileName.lastIndexOf('.'))
                .filter(index -> index > 0)
                .map(index -> fileName.substring(0, index))
                .orElse(fileName);
    }

    public static String getFileExtension(String fileName) {
        return Optional.of(fileName.lastIndexOf('.'))
                .filter(index -> index > 0)
                .map(fileName::substring)
                .orElse("");
    }

    public static Map<String, Object> buildFileMetadata(Path sourceFile) {
        var metadata = new HashMap<String, Object>();
        try {
            var sourceAttrs = Files.readAttributes(sourceFile, BasicFileAttributes.class);
            metadata.put("originalSize", sourceAttrs.size());
            metadata.put("originalLastModified", sourceAttrs.lastModifiedTime().toString());
        } catch (Exception e) {
            log.debug("Could not read source file attributes for {}", sourceFile, e);
        }
        return metadata;
    }

    public static void updateMetadataWithTarget(Map<String, Object> metadata, Path targetFile) {
        try {
            var targetAttrs = Files.readAttributes(targetFile, BasicFileAttributes.class);
            metadata.put("processedSize", targetAttrs.size());
            metadata.put("processedAt", targetAttrs.creationTime().toString());
        } catch (Exception e) {
            log.debug("Could not read target file attributes for {}", targetFile, e);
        }
    }
}
