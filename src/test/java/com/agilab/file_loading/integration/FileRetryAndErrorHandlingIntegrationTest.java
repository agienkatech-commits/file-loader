package com.agilab.file_loading.integration;

import com.agilab.file_loading.FileProcessor;
import com.agilab.file_loading.config.FileLoaderProperties;
import com.agilab.file_loading.event.FileLoadedEvent;
import com.agilab.file_loading.notification.FileNotificationProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.Message;
import org.springframework.test.context.TestPropertySource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Integration test for retry logic and error handling.
 * Tests file processing retry mechanisms and failure scenarios.
 */
@SpringBootTest
@Import(TestChannelBinderConfiguration.class)
@TestPropertySource(properties = {
        "spring.cloud.stream.bindings.fileNotification1-out-0.destination=test-topic1",
        "spring.cloud.stream.bindings.fileNotification1-out-0.content-type=application/json",
        "file-loader.new-subdirectory=new",
        "file-loader.loading-subdirectory=loading",
        "file-loader.loaded-subdirectory=loaded",
        "file-loader.retry-attempts=3",
        "file-loader.retry-delay=PT0.1S"
})
class FileRetryAndErrorHandlingIntegrationTest {

    @Autowired
    private OutputDestination outputDestination;

    @SpyBean
    private FileNotificationProducer notificationProducer;

    @Autowired
    private com.agilab.file_loading.FileProcessingService fileProcessingService;

    @Autowired
    private FileProcessor fileProcessor;

    @Autowired
    private FileLoaderProperties properties;

    @TempDir
    Path tempDir;

    private Path baseDir;
    private Path newDir;
    private Path loadingDir;
    private Path loadedDir;

    @BeforeEach
    void setUp() throws IOException {
        baseDir = tempDir.resolve("test-dir");
        newDir = baseDir.resolve("new");
        loadingDir = baseDir.resolve("loading");
        loadedDir = baseDir.resolve("loaded");
        
        Files.createDirectories(newDir);
        Files.createDirectories(loadingDir);
        Files.createDirectories(loadedDir);
        
        // Configure the test directory dynamically
        Map<String, String> sourceDirectories = new HashMap<>();
        sourceDirectories.put(baseDir.toString(), "fileNotification1-out-0");
        properties.setSourceDirectories(sourceDirectories);
        
        // Clear any previous messages
        while (outputDestination.receive(10, "test-topic1") != null) {
            // drain
        }
    }

    @Test
    void shouldNotMoveFileToLoadedWhenNotificationFails() throws IOException, InterruptedException {
        // Given - setup notification to fail
        doReturn(false).when(notificationProducer).sendFileNotification(any(FileLoadedEvent.class));
        
        Path testFile = Files.createFile(newDir.resolve("test-file.txt"));
        Files.write(testFile, "test content".getBytes());
        
        // Wait for file to be stable
        Thread.sleep(1100);

        // When - process files
        fileProcessingService.processNewFiles();

        // Then - file should be in loading directory, not loaded
        Thread.sleep(500); // Give time for processing
        
        assertThat(Files.exists(testFile)).isFalse();
        assertThat(Files.list(loadingDir).count()).isEqualTo(1);
        assertThat(Files.list(loadedDir).count()).isEqualTo(0);
        
        // Verify notification was attempted
        verify(notificationProducer, times(1)).sendFileNotification(any(FileLoadedEvent.class));
        
        // No message should be in the queue
        Message<?> message = outputDestination.receive(100, "test-topic1");
        assertThat(message).isNull();
    }

    @Test
    void shouldHandleMultipleFilesWithMixedSuccessAndFailure() throws IOException, InterruptedException {
        // Given - setup notification to fail for first call, then succeed
        doReturn(false, true, true)
                .when(notificationProducer).sendFileNotification(any(FileLoadedEvent.class));
        
        Path file1 = Files.createFile(newDir.resolve("file1.txt"));
        Files.write(file1, "content1".getBytes());
        Thread.sleep(100);
        
        Path file2 = Files.createFile(newDir.resolve("file2.txt"));
        Files.write(file2, "content2".getBytes());
        Thread.sleep(100);
        
        Path file3 = Files.createFile(newDir.resolve("file3.txt"));
        Files.write(file3, "content3".getBytes());
        
        // Wait for files to be stable
        Thread.sleep(1100);

        // When - process files
        fileProcessingService.processNewFiles();

        // Then - verify results
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            long loadingCount = Files.list(loadingDir).count();
            long loadedCount = Files.list(loadedDir).count();
            // One file in loading (failed), two in loaded (succeeded)
            assertThat(loadingCount).isEqualTo(1);
            assertThat(loadedCount).isEqualTo(2);
        });
        
        // Verify notification producer was called 3 times (1 failed, 2 succeeded)
        verify(notificationProducer, times(3)).sendFileNotification(any(FileLoadedEvent.class));
    }

    @Test
    void shouldVerifyRetryConfiguration() {
        // Then - verify retry configuration is as expected
        assertThat(properties.getRetryAttempts()).isEqualTo(3);
        assertThat(properties.getRetryDelay()).isEqualTo(Duration.ofMillis(100));
    }
}
