package com.agilab.file_loading;

import com.agilab.file_loading.config.FileLoaderProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FileProcessingServiceTest {

    @Mock
    private FileProcessor fileProcessor;

    private FileLoaderProperties properties;
    private FileProcessingService service;

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
        service = new FileProcessingService(properties, fileProcessor);
    }

    @Test
    void processNewFiles_shouldProcessAllConfiguredDirectories() throws IOException, InterruptedException {
        // Given
        Path dir1 = tempDir.resolve("dir1");
        Path dir2 = tempDir.resolve("dir2");
        Files.createDirectories(dir1.resolve("new"));
        Files.createDirectories(dir2.resolve("new"));
        
        Path file1 = Files.createFile(dir1.resolve("new/file1.txt"));
        Path file2 = Files.createFile(dir2.resolve("new/file2.txt"));
        Files.write(file1, "content1".getBytes());
        Files.write(file2, "content2".getBytes());
        
        Map<String, String> dirs = new HashMap<>();
        dirs.put(dir1.toString(), "binding1");
        dirs.put(dir2.toString(), "binding2");
        properties.setSourceDirectories(dirs);
        
        // Wait for files to be stable
        Thread.sleep(1100);

        // When
        service.processNewFiles();

        // Then
        verify(fileProcessor, times(1)).processFile(eq(file1), eq(dir1.toString()));
        verify(fileProcessor, times(1)).processFile(eq(file2), eq(dir2.toString()));
    }

    @Test
    void processNewFiles_shouldHandleExceptions() throws IOException, InterruptedException {
        // Given
        Path dir1 = tempDir.resolve("dir1");
        Files.createDirectories(dir1.resolve("new"));
        Path file1 = Files.createFile(dir1.resolve("new/file1.txt"));
        Files.write(file1, "content".getBytes());
        
        Map<String, String> dirs = new HashMap<>();
        dirs.put(dir1.toString(), "binding1");
        properties.setSourceDirectories(dirs);
        
        // Wait for file to be stable
        Thread.sleep(1100);
        
        doThrow(new RuntimeException("Processing error"))
                .when(fileProcessor).processFile(any(), any());

        // When - should not throw exception
        service.processNewFiles();

        // Then - verify it was attempted
        verify(fileProcessor, times(1)).processFile(eq(file1), eq(dir1.toString()));
    }
}
