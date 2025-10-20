package com.agilab.file_loading.util;

import com.agilab.file_loading.config.FileLoaderProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.*;

@Slf4j
@RequiredArgsConstructor
@Component
public class FilesHelper {

    private final FileLoaderProperties properties;

    public List<Path> findNewFiles(Path newDirectory) throws IOException {
        try (var files = Files.list(newDirectory)) {
            return files
                    .filter(Files::isRegularFile)
                    .filter(this::isFileStable) // Check if file is fully written
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
                    .toList();
        }
    }

    private boolean isFileStable(Path file) {
        try {
            // Check if file size is stable (not being written to)
            var size1 = Files.size(file);
            Thread.sleep(properties.getFileStabilityCheckDelay().toMillis());
            var size2 = Files.size(file);

            return size1 == size2 && size1 > 0;
        } catch (Exception e) {
            log.debug("File stability check failed for: {}", file, e);
            return false;
        }
    }

    public void moveFileAtomically(Path source, Path target) throws IOException {
        try {
            // Try atomic move first (works on most mounted volumes)
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            log.debug("Atomic move not supported, falling back to copy+delete for: {}", source);
            // Fallback: copy then delete (for blob storage that doesn't support atomic moves)
            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
            Files.delete(source);
        }
    }

    public String getNameWithoutExtension(String fileName) {
        var lastDotIndex = fileName.lastIndexOf('.');
        return switch (lastDotIndex) {
            case -1, 0 -> fileName;
            default -> fileName.substring(0, lastDotIndex);
        };
    }

    public String getFileExtension(String fileName) {
        var lastDotIndex = fileName.lastIndexOf('.');
        return switch (lastDotIndex) {
            case -1, 0 -> "";
            default -> fileName.substring(lastDotIndex);
        };
    }

    public Map<String, Object> buildFileMetadata(Path sourceFile) {
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

    public void updateMetadataWithTarget(Map<String, Object> metadata, Path targetFile) {
        try {
            var targetAttrs = Files.readAttributes(targetFile, BasicFileAttributes.class);
            metadata.put("processedSize", targetAttrs.size());
            metadata.put("processedAt", targetAttrs.creationTime().toString());
        } catch (Exception e) {
            log.debug("Could not read target file attributes for {}", targetFile, e);
        }
    }
}
