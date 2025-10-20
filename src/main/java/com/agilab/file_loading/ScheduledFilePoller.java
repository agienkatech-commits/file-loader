package com.agilab.file_loading;

import module java.base;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;


@Component
@Slf4j
public class ScheduledFilePoller {

    private final FileProcessingService fileProcessingService;

    public ScheduledFilePoller(FileProcessingService fileProcessingService) {
        this.fileProcessingService = fileProcessingService;
    }

    @Scheduled(fixedRateString = "#{@fileLoaderProperties.pollingInterval.toMillis()}")
    public void pollBlobContainers() {
        try {
            log.debug("Polling blob containers for new files...");
            fileProcessingService.processNewFiles();
        } catch (Exception e) {
            log.error("Error during blob polling", e);
        }
    }
}
