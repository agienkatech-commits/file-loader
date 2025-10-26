package com.agilab.file_loading;

import module java.base;
import com.agilab.file_loading.config.FileLoaderProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import static com.agilab.file_loading.util.FileOperations.findNewFiles;

@Service
@Slf4j
public class FileProcessingService {

    private final FileLoaderProperties properties;
    private final FileProcessor fileProcessor;
    private final int maxConcurrentDirs;

    public FileProcessingService(FileLoaderProperties properties, FileProcessor fileProcessor, @Value("${max.concurrent.dirs:100}") int maxConcurrentDirs) {
        this.properties = properties;
        this.fileProcessor = fileProcessor;
        this.maxConcurrentDirs = maxConcurrentDirs;
    }

    public void processNewFiles() {
        properties.getSourceDirectories().keySet().stream().gather(Gatherers.mapConcurrent(maxConcurrentDirs, this::processDirectory))
                .forEach(entry -> log.info("Processed {} new files in directory: {}", entry.getRight(), entry.getLeft()));
    }

    private Pair<String, Integer> processDirectory(String baseDirectory) {
        try {

            log.info("processing new files in directory: {}", baseDirectory);
            var newFilesPath = Paths.get(baseDirectory, properties.getNewSubdirectory());
            var newFiles = findNewFiles(newFilesPath);
            newFiles.forEach(file -> fileProcessor.processFile(file, baseDirectory));
            return Pair.of(baseDirectory, newFiles.size());
        } catch (Exception e) {
            log.error("Error processing directory: {}", baseDirectory, e);
        }
        return Pair.of(baseDirectory, 0);
    }
}