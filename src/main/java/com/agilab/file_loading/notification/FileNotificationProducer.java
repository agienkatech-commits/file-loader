package com.agilab.file_loading.notification;

import module java.base;
import com.agilab.file_loading.config.FileLoaderProperties;
import com.agilab.file_loading.event.FileLoadedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class FileNotificationProducer {

    private final StreamBridge streamBridge;
    private final FileLoaderProperties fileLoaderProperties;

    // Return boolean to indicate success or failure
    public boolean sendFileNotification(FileLoadedEvent event) {
        return resolveBindingAndSend(event)
                .map(binding -> {
                    log.info("File notification sent to binding {}: {}", binding, event.loadedFilePath());
                    return true;
                })
                .orElseGet(() -> {
                    log.error("Failed to resolve binding for directory: {}", event.baseDirectory());
                    return false;
                });
    }

    private Optional<String> resolveBindingAndSend(FileLoadedEvent event) {
        return Optional.ofNullable(fileLoaderProperties.getSourceDirectories().get(event.baseDirectory()))
                .filter(binding -> sendToBinding(binding, event));
    }

    private boolean sendToBinding(String binding, FileLoadedEvent event) {
        try {
            var sent = streamBridge.send(binding, event);
            if (!sent) {
                log.error("Failed to send file notification to binding {}: {}", binding, event.loadedFilePath());
            }
            return sent;
        } catch (Exception e) {
            log.error("Error sending file notification to binding {}: {}", binding, event.loadedFilePath(), e);
            return false;
        }
    }
}