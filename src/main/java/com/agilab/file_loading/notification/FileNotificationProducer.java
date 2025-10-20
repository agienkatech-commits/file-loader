package com.agilab.file_loading.notification;

import com.agilab.file_loading.config.FileLoaderProperties;
import com.agilab.file_loading.event.FileProcessedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class FileNotificationProducer {

    private final StreamBridge streamBridge;
    private final FileLoaderProperties fileLoaderProperties;

    // Return boolean to indicate success or failure
    public boolean sendFileNotification(FileProcessedEvent event) {
        return resolveBindingAndSend(event)
                .map(binding -> {
                    log.info("File notification sent to binding {}: {}", binding, event.getProcessedFilePath());
                    return true;
                })
                .orElseGet(() -> {
                    log.error("Failed to resolve binding for directory: {}", event.getSourceDirectory());
                    return false;
                });
    }

    public void sendFileNotifications(List<FileProcessedEvent> events) {
        var failedCount = events.stream()
                .filter(event -> !sendFileNotification(event))
                .count();
        
        if (failedCount > 0) {
            log.warn("Failed to send {} out of {} notifications", failedCount, events.size());
        }
    }

    private Optional<String> resolveBindingAndSend(FileProcessedEvent event) {
        return Optional.ofNullable(fileLoaderProperties.getSourceDirectories().get(event.getSourceDirectory()))
                .filter(binding -> sendToBinding(binding, event));
    }

    private boolean sendToBinding(String binding, FileProcessedEvent event) {
        try {
            boolean sent = streamBridge.send(binding, event);
            if (!sent) {
                log.error("Failed to send file notification to binding {}: {}", binding, event.getProcessedFilePath());
            }
            return sent;
        } catch (Exception e) {
            log.error("Error sending file notification to binding {}: {}", binding, event.getProcessedFilePath(), e);
            return false;
        }
    }
}