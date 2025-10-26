package com.agilab.file_loading;

import com.agilab.file_loading.config.FileLoaderProperties;
import com.agilab.file_loading.event.FileLoadedEvent;
import com.agilab.file_loading.notification.FileNotificationProducer;
import com.agilab.file_loading.util.FileOperations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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
    private FileOperations fileOperations;

    private FileLoaderProperties properties;
    private FileProcessor fileProcessor;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        properties = new FileLoaderProperties();
        properties.setNewSubdirectory("flow1/new");
        properties.setLoadingSubdirectory("loading");
        properties.setLoadedSubdirectory("flow1/loaded");
        properties.setRetryAttempts(3);
        properties.setRetryDelay(Duration.ofMillis(100));
        fileProcessor = new FileProcessor(notificationProducer, fileOperations, properties);
    }

    @Test
    void testProcessFileSuccessfully() throws IOException {
        // Arrange
        Path sourceFile = tempDir.resolve("test.txt");
        Files.createFile(sourceFile);
        when(notificationProducer.sendFileNotification(any(FileLoadedEvent.class))).thenReturn(true);

        // Act
        fileProcessor.processFile(sourceFile, tempDir.toString());

        // Assert
        verify(fileOperations, times(2)).moveFileAtomicallyWithRetry(any(Path.class), any(Path.class));
        verify(notificationProducer, times(1)).sendFileNotification(any(FileLoadedEvent.class));
    }

    @Test
    void testProcessFileWhenNotificationFails() throws IOException {
        // Arrange
        Path sourceFile = tempDir.resolve("test.txt");
        Files.createFile(sourceFile);
        when(notificationProducer.sendFileNotification(any(FileLoadedEvent.class))).thenReturn(false);

        // Act
        fileProcessor.processFile(sourceFile, tempDir.toString());

        // Assert
        verify(fileOperations, times(1)).moveFileAtomicallyWithRetry(any(Path.class), any(Path.class));
        verify(notificationProducer, times(1)).sendFileNotification(any(FileLoadedEvent.class));
    }

    @Test
    void testProcessFileCreatesLoadedEventWithCorrectData() throws IOException {
        // Arrange
        Path sourceFile = tempDir.resolve("document.pdf");
        Files.createFile(sourceFile);
        ArgumentCaptor<FileLoadedEvent> eventCaptor = ArgumentCaptor.forClass(FileLoadedEvent.class);
        when(notificationProducer.sendFileNotification(any(FileLoadedEvent.class))).thenReturn(true);

        // Act
        fileProcessor.processFile(sourceFile, tempDir.toString());

        // Assert
        verify(notificationProducer).sendFileNotification(eventCaptor.capture());
        FileLoadedEvent capturedEvent = eventCaptor.getValue();
        assertThat(capturedEvent.originalFilePath()).contains("document.pdf");
        assertThat(capturedEvent.baseDirectory()).isEqualTo(tempDir.toString());
    }

    @Test
    void testProcessFileHandlesException() {
        // Arrange
        Path sourceFile = tempDir.resolve("test.txt");
        doThrow(new RuntimeException("Move failed")).when(fileOperations)
                .moveFileAtomicallyWithRetry(any(Path.class), any(Path.class));

        // Act & Assert - should not throw exception
        fileProcessor.processFile(sourceFile, tempDir.toString());

        verify(notificationProducer, never()).sendFileNotification(any());
    }

    @Test
    void testProcessFileGeneratesNewFileNameWithTimestamp() throws IOException {
        // Arrange
        Path sourceFile = tempDir.resolve("data.csv");
        Files.createFile(sourceFile);
        ArgumentCaptor<FileLoadedEvent> eventCaptor = ArgumentCaptor.forClass(FileLoadedEvent.class);
        when(notificationProducer.sendFileNotification(any(FileLoadedEvent.class))).thenReturn(true);

        // Act
        fileProcessor.processFile(sourceFile, tempDir.toString());

        // Assert
        verify(notificationProducer).sendFileNotification(eventCaptor.capture());
        FileLoadedEvent capturedEvent = eventCaptor.getValue();
        assertThat(capturedEvent.fileName()).startsWith("data-").endsWith(".csv");
        assertThat(capturedEvent.fileName()).doesNotContain(":");
    }

    @Test
    void testProcessFileMovesToLoadingDirectoryFirst() throws IOException {
        // Arrange
        Path sourceFile = tempDir.resolve("test.txt");
        Files.createFile(sourceFile);
        ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);
        when(notificationProducer.sendFileNotification(any(FileLoadedEvent.class))).thenReturn(true);

        // Act
        fileProcessor.processFile(sourceFile, tempDir.toString());

        // Assert
        verify(fileOperations, times(2)).moveFileAtomicallyWithRetry(any(Path.class), pathCaptor.capture());
        Path firstMoveDest = pathCaptor.getAllValues().get(0);
        assertThat(firstMoveDest.toString()).contains("loading");
    }

}
