package com.agilab.file_loading;

import com.agilab.file_loading.config.FileLoaderProperties;
import java.nio.file.Paths;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import static com.agilab.file_loading.util.FilesHelper.findNewFiles;

@Service
@Slf4j
@RequiredArgsConstructor
public class FileProcessingService {

    private final FileLoaderProperties properties;
    private final FileProcessor fileProcessor;

    public void processNewFiles() {
        properties.getSourceDirectories().entrySet().stream().parallel()
                .forEach(entry -> processDirectory(entry.getKey()));
    }

    private void processDirectory(String baseDirectory) {
        try {
            var newFilesPath = Paths.get(baseDirectory, properties.getNewSubdirectory());
            findNewFiles(newFilesPath)
                    .forEach(file -> fileProcessor.processFile(file, baseDirectory));

        } catch (Exception e) {
            //TODO add sending to error channel
            log.error("Error processing directory: {}", baseDirectory, e);
        }
    }
}