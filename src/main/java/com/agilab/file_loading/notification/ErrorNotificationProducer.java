package com.agilab.file_loading.notification;

import com.agilab.file_loading.event.FileProcessingErrorEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class ErrorNotificationProducer {

    private static final String ERROR_CHANNEL = "fileProcessingError-out-0";
    private final StreamBridge streamBridge;

    public void sendErrorNotification(FileProcessingErrorEvent event) {
        try {
            var sent = streamBridge.send(ERROR_CHANNEL, event);
            if (sent) {
                log.info("Error notification sent to {}: {}", ERROR_CHANNEL, event.filePath());
            } else {
                log.error("Failed to send error notification to {}: {}", ERROR_CHANNEL, event.filePath());
            }
        } catch (Exception e) {
            log.error("Exception sending error notification to {}: {}", ERROR_CHANNEL, event.filePath(), e);
        }
    }
}
