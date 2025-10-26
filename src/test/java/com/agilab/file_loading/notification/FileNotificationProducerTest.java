package com.agilab.file_loading.notification;

import com.agilab.file_loading.config.FileLoaderProperties;
import com.agilab.file_loading.event.FileLoadedEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cloud.stream.function.StreamBridge;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FileNotificationProducerTest {

    @Mock
    private StreamBridge streamBridge;

    private FileLoaderProperties properties;
    private FileNotificationProducer producer;

    @BeforeEach
    void setUp() {
        properties = new FileLoaderProperties();
        Map<String, String> directories = new HashMap<>();
        directories.put("/path/to/dir1", "binding1-out-0");
        directories.put("/path/to/dir2", "binding2-out-0");
        properties.setSourceDirectories(directories);

        producer = new FileNotificationProducer(streamBridge, properties);
    }

    @Test
    void sendFileNotification_shouldReturnTrueWhenSuccessful() {
        // Given
        FileLoadedEvent event = new FileLoadedEvent(
                "/path/to/dir1/new/file.txt",
                "/path/to/dir1/loaded/file-timestamp.txt",
                "/path/to/dir1",
                Instant.now(),
                "file-timestamp.txt",
                new HashMap<>()
        );
        when(streamBridge.send(eq("binding1-out-0"), eq(event))).thenReturn(true);

        // When
        boolean result = producer.sendFileNotification(event);

        // Then
        assertThat(result).isTrue();
        verify(streamBridge, times(1)).send(eq("binding1-out-0"), eq(event));
    }

    @Test
    void sendFileNotification_shouldReturnFalseWhenStreamBridgeFails() {
        // Given
        FileLoadedEvent event = new FileLoadedEvent(
                "/path/to/dir1/new/file.txt",
                "/path/to/dir1/loaded/file-timestamp.txt",
                "/path/to/dir1",
                Instant.now(),
                "file-timestamp.txt",
                new HashMap<>()
        );
        when(streamBridge.send(eq("binding1-out-0"), eq(event))).thenReturn(false);

        // When
        boolean result = producer.sendFileNotification(event);

        // Then
        assertThat(result).isFalse();
        verify(streamBridge, times(1)).send(eq("binding1-out-0"), eq(event));
    }

    @Test
    void sendFileNotification_shouldReturnFalseWhenBindingNotFound() {
        // Given
        FileLoadedEvent event = new FileLoadedEvent(
                "/path/to/unknown/new/file.txt",
                "/path/to/unknown/loaded/file-timestamp.txt",
                "/path/to/unknown",
                Instant.now(),
                "file-timestamp.txt",
                new HashMap<>()
        );

        // When
        boolean result = producer.sendFileNotification(event);

        // Then
        assertThat(result).isFalse();
        verify(streamBridge, never()).send(any(), any());
    }

    @Test
    void sendFileNotification_shouldHandleExceptions() {
        // Given
        FileLoadedEvent event = new FileLoadedEvent(
                "/path/to/dir1/new/file.txt",
                "/path/to/dir1/loaded/file-timestamp.txt",
                "/path/to/dir1",
                Instant.now(),
                "file-timestamp.txt",
                new HashMap<>()
        );
        when(streamBridge.send(eq("binding1-out-0"), eq(event)))
                .thenThrow(new RuntimeException("Test exception"));

        // When
        boolean result = producer.sendFileNotification(event);

        // Then
        assertThat(result).isFalse();
        verify(streamBridge, times(1)).send(eq("binding1-out-0"), eq(event));
    }
}
