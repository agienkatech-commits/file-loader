package com.agilab.file_loading.notification;

import module java.base;

import com.agilab.file_loading.config.FileLoaderProperties;
import com.agilab.file_loading.event.FileProcessedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class FileNotificationProducer {

    private final StreamBridge streamBridge;
    private final FileLoaderProperties fileLoaderProperties;

    public FileNotificationProducer(StreamBridge streamBridge, FileLoaderProperties fileLoaderProperties) {
        this.streamBridge = streamBridge;
        this.fileLoaderProperties = fileLoaderProperties;
    }

    public void sendFileNotification(FileProcessedEvent event) {
        resolveBindingAndSend(event)
                .ifPresentOrElse(
                        binding -> log.info("File notification sent to binding {}: {}",
                                binding, event.getProcessedFilePath()),
                        () -> log.error("Failed to resolve binding for directory: {}",
                                event.getSourceDirectory())
                );
    }

    public void sendFileNotifications(List<FileProcessedEvent> events) {
        events.stream()
                .forEach(this::sendFileNotification);
    }

    private Optional<String> resolveBindingAndSend(FileProcessedEvent event) {
        return Optional.ofNullable(fileLoaderProperties.getSourceDirectories().get(event.getSourceDirectory()))
                .filter(binding -> sendToBinding(binding, event))
                .or(() -> {
                    log.warn("No binding configured for directory: {}", event.getSourceDirectory());
                    return Optional.empty();
                });
    }

    private boolean sendToBinding(String binding, FileProcessedEvent event) {
        try {
            boolean sent = streamBridge.send(binding, event);
            if (!sent) {
                log.error("Failed to send file notification to binding {}: {}",
                        binding, event.getProcessedFilePath());
            }
            return sent;
        } catch (Exception e) {
            log.error("Error sending file notification to binding {}: {}",
                    binding, event.getProcessedFilePath(), e);
            return false;
        }
    }
}
