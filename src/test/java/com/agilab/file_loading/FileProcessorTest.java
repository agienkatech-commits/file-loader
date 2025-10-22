package com.agilab.file_loading;

import com.agilab.file_loading.config.FileLoaderProperties;
import com.agilab.file_loading.event.FileLoadedEvent;
import com.agilab.file_loading.notification.FileNotificationProducer;
import com.agilab.file_loading.util.FilesOperations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.retry.support.RetryTemplate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FileProcessorTest {

    @Mock
    private FileNotificationProducer notificationProducer;

    @Mock
    private RetryTemplate retryTemplate;

    @Mock
    private FilesOperations filesOperations;

    private FileLoaderProperties properties;
    private FileProcessor fileProcessor;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        properties = new FileLoaderProperties();
        properties.setNewSubdirectory("new");
        properties.setLoadingSubdirectory("loading");
        properties.setLoadedSubdirectory("loaded");
        properties.setRetryAttempts(3);
        properties.setRetryDelay(Duration.ofMillis(100));
        fileProcessor = new FileProcessor(notificationProducer, filesOperations, properties);
    }

    @Test
    void processFile_shouldMoveFileAndSendNotification() throws Exception {
        // Given
        Path baseDir = tempDir.resolve("base");
        Files.createDirectories(baseDir.resolve("new"));
        Files.createDirectories(baseDir.resolve("loading"));
        Files.createDirectories(baseDir.resolve("loaded"));
        
        Path sourceFile = Files.createFile(baseDir.resolve("new/test.txt"));
        Files.write(sourceFile, "test content".getBytes());
        
        when(notificationProducer.sendFileNotification(any())).thenReturn(true);
        doNothing().when(filesOperations).moveFileAtomicallyWithRetry(any(), any());

        // When
        fileProcessor.processFile(sourceFile, baseDir.toString());

        // Then
        verify(notificationProducer, times(1)).sendFileNotification(any(FileLoadedEvent.class));
        verify(filesOperations, times(2)).moveFileAtomicallyWithRetry(any(), any()); // once for loading, once for loaded
    }

    @Test
    void processFile_shouldNotMoveToLoadedIfNotificationFails() throws Exception {
        // Given
        Path baseDir = tempDir.resolve("base");
        Files.createDirectories(baseDir.resolve("new"));
        Files.createDirectories(baseDir.resolve("loading"));
        Files.createDirectories(baseDir.resolve("loaded"));
        
        Path sourceFile = Files.createFile(baseDir.resolve("new/test.txt"));
        Files.write(sourceFile, "test content".getBytes());
        
        when(notificationProducer.sendFileNotification(any())).thenReturn(false);
        doNothing().when(filesOperations).moveFileAtomicallyWithRetry(any(), any());

        // When
        fileProcessor.processFile(sourceFile, baseDir.toString());

        // Then
        verify(notificationProducer, times(1)).sendFileNotification(any(FileLoadedEvent.class));
        verify(filesOperations, times(1)).moveFileAtomicallyWithRetry(any(), any()); // only once for loading, not for loaded
    }

    @Test
    void processFile_shouldCreateEventWithCorrectData() throws Exception {
        // Given
        Path baseDir = tempDir.resolve("base");
        Files.createDirectories(baseDir.resolve("new"));
        Files.createDirectories(baseDir.resolve("loading"));
        Files.createDirectories(baseDir.resolve("loaded"));
        
        Path sourceFile = Files.createFile(baseDir.resolve("new/original.txt"));
        Files.write(sourceFile, "test content".getBytes());
        
        ArgumentCaptor<FileLoadedEvent> eventCaptor = ArgumentCaptor.forClass(FileLoadedEvent.class);
        when(notificationProducer.sendFileNotification(eventCaptor.capture())).thenReturn(true);
        doNothing().when(filesOperations).moveFileAtomicallyWithRetry(any(), any());

        // When
        fileProcessor.processFile(sourceFile, baseDir.toString());

        // Then
        FileLoadedEvent event = eventCaptor.getValue();
        assertThat(event.originalFilePath()).isEqualTo(sourceFile.toString());
        assertThat(event.baseDirectory()).isEqualTo(baseDir.toString());
        assertThat(event.fileName()).startsWith("original-");
        assertThat(event.fileName()).endsWith(".txt");
    }

    @Test
    void processFile_shouldHandleExceptions() throws Exception {
        // Given
        Path baseDir = tempDir.resolve("base");
        Files.createDirectories(baseDir.resolve("new"));
        Files.createDirectories(baseDir.resolve("loading"));
        
        Path sourceFile = Files.createFile(baseDir.resolve("new/test.txt"));
        Files.write(sourceFile, "test content".getBytes());
        
        doThrow(new RuntimeException("Test exception")).when(filesOperations).moveFileAtomicallyWithRetry(any(), any());

        // When - should not throw exception
        fileProcessor.processFile(sourceFile, baseDir.toString());

        // Then
        verify(filesOperations, times(1)).moveFileAtomicallyWithRetry(any(), any());
        verify(notificationProducer, never()).sendFileNotification(any());
    }
}
