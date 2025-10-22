package com.agilab.file_loading;

import com.agilab.file_loading.config.FileLoaderProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Paths;

@Service
@Slf4j
@RequiredArgsConstructor
public class ScheduledFileCleaner {

    private final ElasticsearchClient elasticsearchClient;
    private final FileLoaderProperties properties;

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
                    .filter(this::isMessageInElasticsearch)
                    .forEach(file -> moveToLoaded(file, loadedDir));

        } catch (Exception e) {
            log.error("Error cleaning loading directory: {}", baseDirectory, e);
        }
    }

    private boolean isMessageInElasticsearch(Path file) {
        try {
            var messageId = generateMessageId(file);
            return searchMessageInElasticsearch(messageId);
        } catch (Exception e) {
            log.warn("Failed to check message in Elasticsearch for file: {}", file, e);
            return false;
        }
    }
}