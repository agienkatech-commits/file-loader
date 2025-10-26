package com.agilab.file_loading.integration;

import com.agilab.file_loading.config.FileLoaderProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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

/**
 * Integration test for end-to-end file processing with message sending.
 * Tests successful file moves from new -> loading -> loaded and notification sending.
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
class FileProcessingIntegrationTest {

    @Autowired
    private OutputDestination outputDestination;

    @Autowired
    private com.agilab.file_loading.FileProcessingService fileProcessingService;

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
        newDir = baseDir.resolve("flow1/new");
        loadingDir = baseDir.resolve("loading");
        loadedDir = baseDir.resolve("flow1/loaded");

        Files.createDirectories(newDir);
        Files.createDirectories(loadingDir);
        Files.createDirectories(loadedDir);

        // Configure the test directory dynamically
        Map<String, String> sourceDirectories = new HashMap<>();
        sourceDirectories.put(baseDir.toString(), "fileNotification1-out-0");
        properties.setSourceDirectories(sourceDirectories);
    }

    @Test
    void shouldProcessFileEndToEndAndSendNotification() throws IOException, InterruptedException {
        // Given - create a file in the new directory
        Path testFile = Files.createFile(newDir.resolve("test-file.txt"));
        Files.write(testFile, "test content for integration".getBytes());

        // Wait for file to be stable
        Thread.sleep(1100);

        // When - process files
        fileProcessingService.processNewFiles();

        // Then - verify file moved to loaded directory
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(Files.list(loadedDir).count()).isEqualTo(1);
        });

        assertThat(Files.exists(testFile)).isFalse();
        assertThat(Files.exists(newDir.resolve("test-file.txt"))).isFalse();
        assertThat(Files.exists(loadingDir.resolve("test-file.txt"))).isFalse();

        // Verify notification was sent
        Message<?> message = outputDestination.receive(1000, "test-topic1");
        assertThat(message).isNotNull();
        assertThat(message.getPayload()).isNotNull();
    }

    @Test
    void shouldProcessMultipleFilesInOrder() throws IOException, InterruptedException {
        // Given - create multiple files
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

        // Then - verify all files moved to loaded directory
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(Files.list(loadedDir).count()).isEqualTo(3);
        });

        // Verify 3 notifications were sent
        Message<?> message1 = outputDestination.receive(1000, "test-topic1");
        Message<?> message2 = outputDestination.receive(1000, "test-topic1");
        Message<?> message3 = outputDestination.receive(1000, "test-topic1");

        assertThat(message1).isNotNull();
        assertThat(message2).isNotNull();
        assertThat(message3).isNotNull();
    }

    @Test
    void shouldExcludeTemporaryAndEmptyFiles() throws IOException, InterruptedException {
        // Given - create various files including temp and empty
        Path normalFile = Files.createFile(newDir.resolve("normal.txt"));
        Files.write(normalFile, "content".getBytes());

        Path emptyFile = Files.createFile(newDir.resolve("empty.txt"));
        Path dotFile = Files.createFile(newDir.resolve(".hidden"));
        Files.write(dotFile, "hidden".getBytes());

        Path tmpFile = Files.createFile(newDir.resolve("temp.tmp"));
        Files.write(tmpFile, "temp".getBytes());

        // Wait for files to be stable
        Thread.sleep(1100);

        // When - process files
        fileProcessingService.processNewFiles();

        // Then - only normal file should be processed
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(Files.list(loadedDir).count()).isEqualTo(1);
        });

        // Verify only 1 notification was sent
        Message<?> message = outputDestination.receive(1000, "test-topic1");
        assertThat(message).isNotNull();
        assertThat(message.getPayload()).isNotNull();

        // No more messages
        Message<?> noMessage = outputDestination.receive(100, "test-topic1");
        assertThat(noMessage).isNull();
    }
}
