package com.agilab.file_loading;

import com.agilab.file_loading.config.FileLoaderProperties;
import com.agilab.file_loading.event.FileProcessedEvent;
import com.agilab.file_loading.util.FilesHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@Service
@Slf4j
@RequiredArgsConstructor
public class FileProcessingService {


    private final FileLoaderProperties properties;
    private final FileProcessor fileProcessor;
    private final FilesHelper filesHelper;

    
    public List<FileProcessedEvent> processNewFiles() {
        return properties.getSourceDirectories().entrySet().stream()
            .flatMap(entry -> processDirectoryEntry(entry.getKey()))
            .toList();
    }
    
    private Stream<FileProcessedEvent> processDirectoryEntry(String baseDirectory) {
        try {
            var newFilesPath = Paths.get(baseDirectory, properties.getNewSubdirectory());
            
            if (!Files.exists(newFilesPath)) {
                log.debug("New files directory does not exist: {}", newFilesPath);
                return Stream.empty();
            }
            
            return filesHelper.findNewFiles(newFilesPath).stream()
                .limit(properties.getBatchSize()) // Process in batches
                .map(file -> fileProcessor.processFileWithRetry(file, baseDirectory))
                .filter(Optional::isPresent)
                .map(Optional::get);
                
        } catch (Exception e) {
            //TODO add sending to error channel
            log.error("Error processing directory: {}", baseDirectory, e);
            return Stream.empty();
        }
    }
}