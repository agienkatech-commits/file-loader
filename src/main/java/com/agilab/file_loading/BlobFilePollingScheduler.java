package com.agilab.file_loading;

import module java.base;

import com.agilab.file_loading.event.FileProcessedEvent;
import com.agilab.file_loading.notification.FileNotificationProducer;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class BlobFilePollingScheduler {

    private final FileProcessingService fileProcessingService;
    private final FileNotificationProducer notificationProducer;

    public BlobFilePollingScheduler(
            FileProcessingService fileProcessingService,
            FileNotificationProducer notificationProducer) {
        this.fileProcessingService = fileProcessingService;
        this.notificationProducer = notificationProducer;
    }

    @Scheduled(fixedRateString = "#{@fileLoaderProperties.pollingInterval.toMillis()}")
    public void pollBlobContainers() {
        try {
            log.debug("Polling blob containers for new files...");

            List<FileProcessedEvent> events = fileProcessingService.processNewFiles();

            if (!events.isEmpty()) {
                notificationProducer.sendFileNotifications(events);
                log.info("Processed {} new files", events.size());

            }
            if (!events.isEmpty()) {
                notificationProducer.sendFileNotifications(events);
                log.info("Processed {} new blobs", events.size());
            }
        } catch (Exception e) {
            log.error("Error during blob polling", e);
        }
    }
}
