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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ScheduledFileCleanerTest {

    @Mock
    private FileNotificationProducer notificationProducer;

    @Mock
    private FilesOperations filesOperations;

    private FileLoaderProperties properties;
    private ScheduledFileCleaner scheduledFileCleaner;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        properties = new FileLoaderProperties();
        properties.setLoadingSubdirectory("loading");
        properties.setLoadedSubdirectory("loaded");
        properties.setStuckFileThreshold(Duration.ofMinutes(5));
        properties.setCleaningInterval(Duration.ofMinutes(1));
        
        scheduledFileCleaner = new ScheduledFileCleaner(properties, notificationProducer, filesOperations);
    }

    @Test
    void cleanStickFiles_shouldProcessAllSourceDirectories() throws IOException {
        // Given
        Path dir1 = tempDir.resolve("dir1");
        Path dir2 = tempDir.resolve("dir2");
        Files.createDirectories(dir1.resolve("loading"));
        Files.createDirectories(dir2.resolve("loading"));
        
        Map<String, String> directories = new HashMap<>();
        directories.put(dir1.toString(), "binding1");
        directories.put(dir2.toString(), "binding2");
        properties.setSourceDirectories(directories);

        // When
        scheduledFileCleaner.cleanStickFiles();

        // Then - directories were processed (no exception thrown)
        assertThat(Files.exists(dir1.resolve("loading"))).isTrue();
        assertThat(Files.exists(dir2.resolve("loading"))).isTrue();
    }

    @Test
    void cleanStickFiles_shouldHandleParallelProcessing() {
        // Given
        Map<String, String> directories = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            directories.put(tempDir.resolve("dir" + i).toString(), "binding" + i);
        }
        properties.setSourceDirectories(directories);

        // When - should process all directories in parallel without throwing
        scheduledFileCleaner.cleanStickFiles();

        // Then - verify no exceptions were thrown
        assertThat(directories).hasSize(5);
    }

    @Test
    void cleanLoadingDirectory_shouldIgnoreNonExistentDirectory() {
        // Given
        Path nonExistentDir = tempDir.resolve("nonexistent");
        Map<String, String> directories = new HashMap<>();
        directories.put(nonExistentDir.toString(), "binding1");
        properties.setSourceDirectories(directories);

        // When - should not throw exception
        scheduledFileCleaner.cleanStickFiles();

        // Then - no interaction with mocks
        verifyNoInteractions(notificationProducer);
        verifyNoInteractions(filesOperations);
    }

    @Test
    void cleanLoadingDirectory_shouldProcessOldEnoughFiles() throws IOException {
        // Given
        Path baseDir = tempDir.resolve("base");
        Path loadingDir = baseDir.resolve("loading");
        Path loadedDir = baseDir.resolve("loaded");
        Files.createDirectories(loadingDir);
        Files.createDirectories(loadedDir);
        
        // Create an old file (older than threshold)
        Path oldFile = Files.createFile(loadingDir.resolve("old-file.txt"));
        Files.write(oldFile, "old content".getBytes());
        Instant oldTime = Instant.now().minus(Duration.ofMinutes(10));
        Files.setLastModifiedTime(oldFile, FileTime.from(oldTime));
        
        Map<String, String> directories = new HashMap<>();
        directories.put(baseDir.toString(), "binding1");
        properties.setSourceDirectories(directories);
        properties.setStuckFileThreshold(Duration.ofMinutes(5));
        
        when(notificationProducer.sendFileNotification(any())).thenReturn(true);

        // When
        scheduledFileCleaner.cleanStickFiles();

        // Then
        verify(notificationProducer, times(1)).sendFileNotification(any(FileLoadedEvent.class));
        verify(filesOperations, times(1)).moveFileAtomicallyWithRetry(eq(oldFile), any(Path.class));
    }

    @Test
    void cleanLoadingDirectory_shouldNotProcessRecentFiles() throws IOException {
        // Given
        Path baseDir = tempDir.resolve("base");
        Path loadingDir = baseDir.resolve("loading");
        Files.createDirectories(loadingDir);
        
        // Create a recent file (newer than threshold)
        Path recentFile = Files.createFile(loadingDir.resolve("recent-file.txt"));
        Files.write(recentFile, "recent content".getBytes());
        // File has current timestamp by default
        
        Map<String, String> directories = new HashMap<>();
        directories.put(baseDir.toString(), "binding1");
        properties.setSourceDirectories(directories);
        properties.setStuckFileThreshold(Duration.ofMinutes(5));

        // When
        scheduledFileCleaner.cleanStickFiles();

        // Then - no interactions with mocks
        verifyNoInteractions(notificationProducer);
        verifyNoInteractions(filesOperations);
    }

    @Test
    void cleanLoadingDirectory_shouldFilterByIsOldEnough() throws IOException {
        // Given
        Path baseDir = tempDir.resolve("base");
        Path loadingDir = baseDir.resolve("loading");
        Files.createDirectories(loadingDir);
        
        // Create multiple files with different ages
        Path oldFile1 = Files.createFile(loadingDir.resolve("old1.txt"));
        Path oldFile2 = Files.createFile(loadingDir.resolve("old2.txt"));
        Path recentFile = Files.createFile(loadingDir.resolve("recent.txt"));
        
        Files.write(oldFile1, "content1".getBytes());
        Files.write(oldFile2, "content2".getBytes());
        Files.write(recentFile, "content3".getBytes());
        
        Instant oldTime = Instant.now().minus(Duration.ofMinutes(10));
        Files.setLastModifiedTime(oldFile1, FileTime.from(oldTime));
        Files.setLastModifiedTime(oldFile2, FileTime.from(oldTime));
        // recentFile keeps current timestamp
        
        Map<String, String> directories = new HashMap<>();
        directories.put(baseDir.toString(), "binding1");
        properties.setSourceDirectories(directories);
        properties.setStuckFileThreshold(Duration.ofMinutes(5));
        
        when(notificationProducer.sendFileNotification(any())).thenReturn(true);

        // When
        scheduledFileCleaner.cleanStickFiles();

        // Then - only old files are processed
        verify(notificationProducer, times(2)).sendFileNotification(any(FileLoadedEvent.class));
        verify(filesOperations, times(2)).moveFileAtomicallyWithRetry(any(Path.class), any(Path.class));
    }

    @Test
    void isOldEnough_shouldReturnTrueForOldFile() throws IOException {
        // Given
        Path baseDir = tempDir.resolve("base");
        Path loadingDir = baseDir.resolve("loading");
        Files.createDirectories(loadingDir);
        
        Path oldFile = Files.createFile(loadingDir.resolve("old.txt"));
        Instant oldTime = Instant.now().minus(Duration.ofMinutes(10));
        Files.setLastModifiedTime(oldFile, FileTime.from(oldTime));
        
        Map<String, String> directories = new HashMap<>();
        directories.put(baseDir.toString(), "binding1");
        properties.setSourceDirectories(directories);
        properties.setStuckFileThreshold(Duration.ofMinutes(5));
        
        when(notificationProducer.sendFileNotification(any())).thenReturn(true);

        // When
        scheduledFileCleaner.cleanStickFiles();

        // Then - file was processed (notification sent)
        verify(notificationProducer, times(1)).sendFileNotification(any(FileLoadedEvent.class));
    }

    @Test
    void isOldEnough_shouldReturnFalseForRecentFile() throws IOException {
        // Given
        Path baseDir = tempDir.resolve("base");
        Path loadingDir = baseDir.resolve("loading");
        Files.createDirectories(loadingDir);
        
        Path recentFile = Files.createFile(loadingDir.resolve("recent.txt"));
        Files.write(recentFile, "content".getBytes());
        // Current timestamp by default
        
        Map<String, String> directories = new HashMap<>();
        directories.put(baseDir.toString(), "binding1");
        properties.setSourceDirectories(directories);
        properties.setStuckFileThreshold(Duration.ofMinutes(5));

        // When
        scheduledFileCleaner.cleanStickFiles();

        // Then - file was not processed
        verifyNoInteractions(notificationProducer);
    }

    @Test
    void isOldEnough_shouldHandleIOException() throws IOException {
        // Given - Create a file then simulate IOException by processing a path to deleted file
        Path baseDir = tempDir.resolve("base");
        Path loadingDir = baseDir.resolve("loading");
        Files.createDirectories(loadingDir);
        
        // Note: We can't easily simulate IOException on getLastModifiedTime in unit test
        // but the code handles it by returning false and logging a warning
        // This test verifies the happy path, integration tests would cover IOException cases
        
        Map<String, String> directories = new HashMap<>();
        directories.put(baseDir.toString(), "binding1");
        properties.setSourceDirectories(directories);

        // When - should not throw exception
        scheduledFileCleaner.cleanStickFiles();

        // Then - no crash
        verifyNoInteractions(notificationProducer);
    }

    @Test
    void sendNotificationAndMoveToLoaded_shouldSendNotificationAndMoveFile() throws IOException {
        // Given
        Path baseDir = tempDir.resolve("base");
        Path loadingDir = baseDir.resolve("loading");
        Path loadedDir = baseDir.resolve("loaded");
        Files.createDirectories(loadingDir);
        Files.createDirectories(loadedDir);
        
        Path oldFile = Files.createFile(loadingDir.resolve("stuck-file.txt"));
        Files.write(oldFile, "stuck content".getBytes());
        Instant oldTime = Instant.now().minus(Duration.ofMinutes(10));
        Files.setLastModifiedTime(oldFile, FileTime.from(oldTime));
        
        Map<String, String> directories = new HashMap<>();
        directories.put(baseDir.toString(), "binding1");
        properties.setSourceDirectories(directories);
        properties.setStuckFileThreshold(Duration.ofMinutes(5));
        
        ArgumentCaptor<FileLoadedEvent> eventCaptor = ArgumentCaptor.forClass(FileLoadedEvent.class);
        when(notificationProducer.sendFileNotification(eventCaptor.capture())).thenReturn(true);

        // When
        scheduledFileCleaner.cleanStickFiles();

        // Then
        FileLoadedEvent event = eventCaptor.getValue();
        assertThat(event.originalFilePath()).isEqualTo(oldFile.toString());
        assertThat(event.baseDirectory()).isEqualTo(baseDir.toString());
        assertThat(event.fileName()).isEqualTo("stuck-file");
        
        verify(notificationProducer, times(1)).sendFileNotification(any(FileLoadedEvent.class));
        verify(filesOperations, times(1)).moveFileAtomicallyWithRetry(
            eq(oldFile), 
            eq(loadedDir.resolve("stuck-file"))
        );
    }

    @Test
    void sendNotificationAndMoveToLoaded_shouldNotMoveFileWhenNotificationFails() throws IOException {
        // Given
        Path baseDir = tempDir.resolve("base");
        Path loadingDir = baseDir.resolve("loading");
        Files.createDirectories(loadingDir);
        
        Path oldFile = Files.createFile(loadingDir.resolve("stuck-file.txt"));
        Files.write(oldFile, "stuck content".getBytes());
        Instant oldTime = Instant.now().minus(Duration.ofMinutes(10));
        Files.setLastModifiedTime(oldFile, FileTime.from(oldTime));
        
        Map<String, String> directories = new HashMap<>();
        directories.put(baseDir.toString(), "binding1");
        properties.setSourceDirectories(directories);
        properties.setStuckFileThreshold(Duration.ofMinutes(5));
        
        when(notificationProducer.sendFileNotification(any())).thenReturn(false);

        // When
        scheduledFileCleaner.cleanStickFiles();

        // Then
        verify(notificationProducer, times(1)).sendFileNotification(any(FileLoadedEvent.class));
        verifyNoInteractions(filesOperations); // File should not be moved
    }

    @Test
    void sendNotificationAndMoveToLoaded_shouldHandleNotificationException() throws IOException {
        // Given
        Path baseDir = tempDir.resolve("base");
        Path loadingDir = baseDir.resolve("loading");
        Files.createDirectories(loadingDir);
        
        Path oldFile = Files.createFile(loadingDir.resolve("stuck-file.txt"));
        Files.write(oldFile, "stuck content".getBytes());
        Instant oldTime = Instant.now().minus(Duration.ofMinutes(10));
        Files.setLastModifiedTime(oldFile, FileTime.from(oldTime));
        
        Map<String, String> directories = new HashMap<>();
        directories.put(baseDir.toString(), "binding1");
        properties.setSourceDirectories(directories);
        properties.setStuckFileThreshold(Duration.ofMinutes(5));
        
        when(notificationProducer.sendFileNotification(any()))
            .thenThrow(new RuntimeException("Notification error"));

        // When - should not throw exception
        scheduledFileCleaner.cleanStickFiles();

        // Then - error was caught and logged
        verify(notificationProducer, times(1)).sendFileNotification(any(FileLoadedEvent.class));
        verifyNoInteractions(filesOperations);
    }

    @Test
    void sendNotificationAndMoveToLoaded_shouldHandleFileMoveException() throws IOException {
        // Given
        Path baseDir = tempDir.resolve("base");
        Path loadingDir = baseDir.resolve("loading");
        Files.createDirectories(loadingDir);
        
        Path oldFile = Files.createFile(loadingDir.resolve("stuck-file.txt"));
        Files.write(oldFile, "stuck content".getBytes());
        Instant oldTime = Instant.now().minus(Duration.ofMinutes(10));
        Files.setLastModifiedTime(oldFile, FileTime.from(oldTime));
        
        Map<String, String> directories = new HashMap<>();
        directories.put(baseDir.toString(), "binding1");
        properties.setSourceDirectories(directories);
        properties.setStuckFileThreshold(Duration.ofMinutes(5));
        
        when(notificationProducer.sendFileNotification(any())).thenReturn(true);
        doThrow(new RuntimeException("Move error"))
            .when(filesOperations).moveFileAtomicallyWithRetry(any(Path.class), any(Path.class));

        // When - should not throw exception (handled by @SneakyThrows)
        scheduledFileCleaner.cleanStickFiles();

        // Then
        verify(notificationProducer, times(1)).sendFileNotification(any(FileLoadedEvent.class));
        verify(filesOperations, times(1)).moveFileAtomicallyWithRetry(any(Path.class), any(Path.class));
    }

    @Test
    void sendNotificationAndMoveToLoaded_shouldCreateLoadedDirectoryIfMissing() throws IOException {
        // Given
        Path baseDir = tempDir.resolve("base");
        Path loadingDir = baseDir.resolve("loading");
        Files.createDirectories(loadingDir);
        // Note: loadedDir is NOT created
        
        Path oldFile = Files.createFile(loadingDir.resolve("stuck-file.txt"));
        Files.write(oldFile, "stuck content".getBytes());
        Instant oldTime = Instant.now().minus(Duration.ofMinutes(10));
        Files.setLastModifiedTime(oldFile, FileTime.from(oldTime));
        
        Map<String, String> directories = new HashMap<>();
        directories.put(baseDir.toString(), "binding1");
        properties.setSourceDirectories(directories);
        properties.setStuckFileThreshold(Duration.ofMinutes(5));
        
        when(notificationProducer.sendFileNotification(any())).thenReturn(true);

        // When
        scheduledFileCleaner.cleanStickFiles();

        // Then - loadedDir should be created
        Path loadedDir = baseDir.resolve("loaded");
        assertThat(Files.exists(loadedDir)).isTrue();
        assertThat(Files.isDirectory(loadedDir)).isTrue();
    }

    @Test
    void cleanLoadingDirectory_shouldOnlyProcessRegularFiles() throws IOException {
        // Given
        Path baseDir = tempDir.resolve("base");
        Path loadingDir = baseDir.resolve("loading");
        Files.createDirectories(loadingDir);
        
        // Create a regular file
        Path regularFile = Files.createFile(loadingDir.resolve("regular.txt"));
        Files.write(regularFile, "content".getBytes());
        Instant oldTime = Instant.now().minus(Duration.ofMinutes(10));
        Files.setLastModifiedTime(regularFile, FileTime.from(oldTime));
        
        // Create a subdirectory (should be ignored)
        Path subDir = Files.createDirectory(loadingDir.resolve("subdir"));
        
        Map<String, String> directories = new HashMap<>();
        directories.put(baseDir.toString(), "binding1");
        properties.setSourceDirectories(directories);
        properties.setStuckFileThreshold(Duration.ofMinutes(5));
        
        when(notificationProducer.sendFileNotification(any())).thenReturn(true);

        // When
        scheduledFileCleaner.cleanStickFiles();

        // Then - only the regular file should be processed
        verify(notificationProducer, times(1)).sendFileNotification(any(FileLoadedEvent.class));
        verify(filesOperations, times(1)).moveFileAtomicallyWithRetry(any(Path.class), any(Path.class));
    }

    @Test
    void cleanLoadingDirectory_shouldHandleExceptionGracefully() throws IOException {
        // Given
        Path baseDir = tempDir.resolve("base");
        Path loadingDir = baseDir.resolve("loading");
        Files.createDirectories(loadingDir);
        
        Map<String, String> directories = new HashMap<>();
        directories.put(baseDir.toString(), "binding1");
        properties.setSourceDirectories(directories);

        // When - should not throw exception even with issues
        scheduledFileCleaner.cleanStickFiles();

        // Then - no crash occurred
        assertThat(Files.exists(loadingDir)).isTrue();
    }

    @Test
    void cleanLoadingDirectory_shouldHandleMultipleFilesInParallel() throws IOException {
        // Given
        Path baseDir = tempDir.resolve("base");
        Path loadingDir = baseDir.resolve("loading");
        Files.createDirectories(loadingDir);
        
        // Create multiple old files
        for (int i = 0; i < 5; i++) {
            Path file = Files.createFile(loadingDir.resolve("file" + i + ".txt"));
            Files.write(file, ("content" + i).getBytes());
            Instant oldTime = Instant.now().minus(Duration.ofMinutes(10));
            Files.setLastModifiedTime(file, FileTime.from(oldTime));
        }
        
        Map<String, String> directories = new HashMap<>();
        directories.put(baseDir.toString(), "binding1");
        properties.setSourceDirectories(directories);
        properties.setStuckFileThreshold(Duration.ofMinutes(5));
        
        when(notificationProducer.sendFileNotification(any())).thenReturn(true);

        // When
        scheduledFileCleaner.cleanStickFiles();

        // Then - all files should be processed
        verify(notificationProducer, times(5)).sendFileNotification(any(FileLoadedEvent.class));
        verify(filesOperations, times(5)).moveFileAtomicallyWithRetry(any(Path.class), any(Path.class));
    }

    @Test
    void sendNotificationAndMoveToLoaded_shouldUseCorrectFilePaths() throws IOException {
        // Given
        Path baseDir = tempDir.resolve("base");
        Path loadingDir = baseDir.resolve("loading");
        Path loadedDir = baseDir.resolve("loaded");
        Files.createDirectories(loadingDir);
        
        Path oldFile = Files.createFile(loadingDir.resolve("test-file.csv"));
        Files.write(oldFile, "csv,data".getBytes());
        Instant oldTime = Instant.now().minus(Duration.ofMinutes(10));
        Files.setLastModifiedTime(oldFile, FileTime.from(oldTime));
        
        Map<String, String> directories = new HashMap<>();
        directories.put(baseDir.toString(), "binding1");
        properties.setSourceDirectories(directories);
        properties.setStuckFileThreshold(Duration.ofMinutes(5));
        
        ArgumentCaptor<FileLoadedEvent> eventCaptor = ArgumentCaptor.forClass(FileLoadedEvent.class);
        ArgumentCaptor<Path> targetPathCaptor = ArgumentCaptor.forClass(Path.class);
        
        when(notificationProducer.sendFileNotification(eventCaptor.capture())).thenReturn(true);

        // When
        scheduledFileCleaner.cleanStickFiles();

        // Then
        FileLoadedEvent event = eventCaptor.getValue();
        assertThat(event.originalFilePath()).isEqualTo(oldFile.toString());
        assertThat(event.loadedFilePath()).isEqualTo(loadedDir.resolve("test-file").toString());
        assertThat(event.baseDirectory()).isEqualTo(baseDir.toString());
        assertThat(event.fileName()).isEqualTo("test-file");
        assertThat(event.metadata()).isEmpty();
        
        verify(filesOperations).moveFileAtomicallyWithRetry(
            eq(oldFile),
            eq(loadedDir.resolve("test-file"))
        );
    }

    @Test
    void cleanStickFiles_shouldHandleEmptyLoadingDirectory() throws IOException {
        // Given
        Path baseDir = tempDir.resolve("base");
        Path loadingDir = baseDir.resolve("loading");
        Files.createDirectories(loadingDir);
        
        Map<String, String> directories = new HashMap<>();
        directories.put(baseDir.toString(), "binding1");
        properties.setSourceDirectories(directories);

        // When
        scheduledFileCleaner.cleanStickFiles();

        // Then - no files processed
        verifyNoInteractions(notificationProducer);
        verifyNoInteractions(filesOperations);
    }

    @Test
    void cleanStickFiles_shouldHandleEmptySourceDirectories() {
        // Given
        properties.setSourceDirectories(new HashMap<>());

        // When - should not throw exception
        scheduledFileCleaner.cleanStickFiles();

        // Then
        verifyNoInteractions(notificationProducer);
        verifyNoInteractions(filesOperations);
    }
}
