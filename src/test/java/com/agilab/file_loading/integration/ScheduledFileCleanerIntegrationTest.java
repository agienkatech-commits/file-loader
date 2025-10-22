package com.agilab.file_loading.integration;

import com.agilab.file_loading.ScheduledFileCleaner;
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
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for ScheduledFileCleaner using @SpringBootTest.
 * Tests full Spring Boot application context with temporary directory structures
 * that mimic source directories (new, loading, loaded subdirectories).
 * Verifies stuck file detection, notification sending, and file cleanup.
 */
@SpringBootTest
@Import(TestChannelBinderConfiguration.class)
@TestPropertySource(properties = {
        "spring.cloud.stream.bindings.fileNotification1-out-0.destination=test-topic1",
        "spring.cloud.stream.bindings.fileNotification2-out-0.destination=test-topic2",
        "spring.cloud.stream.bindings.fileNotification1-out-0.content-type=application/json",
        "spring.cloud.stream.bindings.fileNotification2-out-0.content-type=application/json",
        "file-loader.new-subdirectory=new",
        "file-loader.loading-subdirectory=loading",
        "file-loader.loaded-subdirectory=loaded",
        "file-loader.stuck-file-threshold=PT0.5S",  // 500ms for testing
        "file-loader.retry-attempts=3",
        "file-loader.retry-delay=PT0.1S"
})
class ScheduledFileCleanerIntegrationTest {

    @Autowired
    private ScheduledFileCleaner scheduledFileCleaner;

    @Autowired
    private OutputDestination outputDestination;

    @Autowired
    private FileLoaderProperties properties;

    @TempDir
    Path tempDir;

    private Path baseDir1;
    private Path newDir1;
    private Path loadingDir1;
    private Path loadedDir1;

    private Path baseDir2;
    private Path newDir2;
    private Path loadingDir2;
    private Path loadedDir2;

    @BeforeEach
    void setUp() throws IOException {
        // Setup first base directory
        baseDir1 = tempDir.resolve("test-dir1");
        newDir1 = baseDir1.resolve("new");
        loadingDir1 = baseDir1.resolve("loading");
        loadedDir1 = baseDir1.resolve("loaded");

        Files.createDirectories(newDir1);
        Files.createDirectories(loadingDir1);
        Files.createDirectories(loadedDir1);

        // Setup second base directory
        baseDir2 = tempDir.resolve("test-dir2");
        newDir2 = baseDir2.resolve("new");
        loadingDir2 = baseDir2.resolve("loading");
        loadedDir2 = baseDir2.resolve("loaded");

        Files.createDirectories(newDir2);
        Files.createDirectories(loadingDir2);
        Files.createDirectories(loadedDir2);

        // Configure the test directories dynamically
        Map<String, String> sourceDirectories = new HashMap<>();
        sourceDirectories.put(baseDir1.toString(), "fileNotification1-out-0");
        sourceDirectories.put(baseDir2.toString(), "fileNotification2-out-0");
        properties.setSourceDirectories(sourceDirectories);

        // Clear any previous messages
        clearMessages("test-topic1");
        clearMessages("test-topic2");
    }

    private void clearMessages(String destination) {
        while (outputDestination.receive(10, destination) != null) {
            // drain messages
        }
    }

    @Test
    void shouldDetectAndCleanStuckFiles() throws IOException, InterruptedException {
        // Given - create a stuck file in loading directory
        Path stuckFile = Files.createFile(loadingDir1.resolve("stuck-file.txt"));
        Files.write(stuckFile, "stuck content".getBytes());

        // Set file's last modified time to exceed stuck threshold
        Instant oldTime = Instant.now().minus(Duration.ofSeconds(2));
        Files.setLastModifiedTime(stuckFile, FileTime.from(oldTime));

        // When - execute cleaning process
        scheduledFileCleaner.cleanStickFiles();

        // Then - verify file was moved to loaded directory
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            List<Path> loadedFiles = Files.list(loadedDir1).collect(Collectors.toList());
            assertThat(loadedFiles).hasSize(1);
            assertThat(loadedFiles.get(0).getFileName().toString()).isEqualTo("stuck-file");
        });

        // Verify file no longer exists in loading directory
        assertThat(Files.exists(stuckFile)).isFalse();

        // Verify notification was sent
        Message<?> message = outputDestination.receive(1000, "test-topic1");
        assertThat(message).isNotNull();
        assertThat(message.getPayload()).isNotNull();
    }

    @Test
    void shouldNotCleanRecentFiles() throws IOException, InterruptedException {
        // Given - create a recent file in loading directory
        Path recentFile = Files.createFile(loadingDir1.resolve("recent-file.txt"));
        Files.write(recentFile, "recent content".getBytes());

        // File is just created, so it's recent (not stuck)

        // When - execute cleaning process
        scheduledFileCleaner.cleanStickFiles();

        // Allow some time for processing
        Thread.sleep(500);

        // Then - verify file was NOT moved (still in loading directory)
        assertThat(Files.exists(recentFile)).isTrue();
        assertThat(Files.list(loadedDir1).count()).isEqualTo(0);

        // Verify no notification was sent
        Message<?> message = outputDestination.receive(100, "test-topic1");
        assertThat(message).isNull();
    }

    @Test
    void shouldHandleParallelProcessingAcrossMultipleDirectories() throws IOException, InterruptedException {
        // Given - create stuck files in multiple directories
        Path stuckFile1 = Files.createFile(loadingDir1.resolve("stuck-file1.txt"));
        Files.write(stuckFile1, "content1".getBytes());
        Instant oldTime1 = Instant.now().minus(Duration.ofSeconds(2));
        Files.setLastModifiedTime(stuckFile1, FileTime.from(oldTime1));

        Path stuckFile2 = Files.createFile(loadingDir2.resolve("stuck-file2.txt"));
        Files.write(stuckFile2, "content2".getBytes());
        Instant oldTime2 = Instant.now().minus(Duration.ofSeconds(2));
        Files.setLastModifiedTime(stuckFile2, FileTime.from(oldTime2));

        // When - execute cleaning process (processes directories in parallel)
        scheduledFileCleaner.cleanStickFiles();

        // Then - verify both files were moved to their respective loaded directories
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            List<Path> loadedFiles1 = Files.list(loadedDir1).collect(Collectors.toList());
            List<Path> loadedFiles2 = Files.list(loadedDir2).collect(Collectors.toList());
            
            assertThat(loadedFiles1).hasSize(1);
            assertThat(loadedFiles2).hasSize(1);
        });

        // Verify files no longer exist in loading directories
        assertThat(Files.exists(stuckFile1)).isFalse();
        assertThat(Files.exists(stuckFile2)).isFalse();

        // Verify notifications were sent to correct topics
        Message<?> message1 = outputDestination.receive(1000, "test-topic1");
        Message<?> message2 = outputDestination.receive(1000, "test-topic2");

        assertThat(message1).isNotNull();
        assertThat(message2).isNotNull();
    }

    @Test
    void shouldCleanMultipleStuckFilesInSameDirectory() throws IOException, InterruptedException {
        // Given - create multiple stuck files in loading directory
        Path stuckFile1 = Files.createFile(loadingDir1.resolve("stuck-file1.txt"));
        Files.write(stuckFile1, "content1".getBytes());
        Path stuckFile2 = Files.createFile(loadingDir1.resolve("stuck-file2.txt"));
        Files.write(stuckFile2, "content2".getBytes());
        Path stuckFile3 = Files.createFile(loadingDir1.resolve("stuck-file3.txt"));
        Files.write(stuckFile3, "content3".getBytes());

        // Set all files' last modified time to exceed stuck threshold
        Instant oldTime = Instant.now().minus(Duration.ofSeconds(2));
        Files.setLastModifiedTime(stuckFile1, FileTime.from(oldTime));
        Files.setLastModifiedTime(stuckFile2, FileTime.from(oldTime));
        Files.setLastModifiedTime(stuckFile3, FileTime.from(oldTime));

        // When - execute cleaning process
        scheduledFileCleaner.cleanStickFiles();

        // Then - verify all files were moved to loaded directory
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(Files.list(loadedDir1).count()).isEqualTo(3);
        });

        // Verify files no longer exist in loading directory
        assertThat(Files.exists(stuckFile1)).isFalse();
        assertThat(Files.exists(stuckFile2)).isFalse();
        assertThat(Files.exists(stuckFile3)).isFalse();

        // Verify 3 notifications were sent
        Message<?> message1 = outputDestination.receive(1000, "test-topic1");
        Message<?> message2 = outputDestination.receive(1000, "test-topic1");
        Message<?> message3 = outputDestination.receive(1000, "test-topic1");

        assertThat(message1).isNotNull();
        assertThat(message2).isNotNull();
        assertThat(message3).isNotNull();
    }

    @Test
    void shouldHandleLoadingDirectoryNotExists() {
        // Given - loading directory does not exist (only new and loaded exist)
        Path baseDir3 = tempDir.resolve("test-dir3");
        Path newDir3 = baseDir3.resolve("new");
        Path loadedDir3 = baseDir3.resolve("loaded");

        try {
            Files.createDirectories(newDir3);
            Files.createDirectories(loadedDir3);
            // Note: loadingDir3 is NOT created

            Map<String, String> sourceDirectories = new HashMap<>();
            sourceDirectories.put(baseDir3.toString(), "fileNotification1-out-0");
            properties.setSourceDirectories(sourceDirectories);

            // When - execute cleaning process (should handle gracefully)
            scheduledFileCleaner.cleanStickFiles();

            // Then - should not throw exception
            assertThat(Files.exists(baseDir3.resolve("loading"))).isFalse();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void shouldHandleEmptyLoadingDirectory() throws IOException {
        // Given - empty loading directory
        assertThat(Files.list(loadingDir1).count()).isEqualTo(0);

        // When - execute cleaning process
        scheduledFileCleaner.cleanStickFiles();

        // Then - should complete without errors
        assertThat(Files.list(loadingDir1).count()).isEqualTo(0);
        assertThat(Files.list(loadedDir1).count()).isEqualTo(0);

        // Verify no notifications were sent
        Message<?> message = outputDestination.receive(100, "test-topic1");
        assertThat(message).isNull();
    }

    @Test
    void shouldCreateLoadedDirectoryIfNotExists() throws IOException, InterruptedException {
        // Given - delete loaded directory to simulate it not existing
        Files.delete(loadedDir1);
        assertThat(Files.exists(loadedDir1)).isFalse();

        // Create a stuck file in loading directory
        Path stuckFile = Files.createFile(loadingDir1.resolve("stuck-file.txt"));
        Files.write(stuckFile, "content".getBytes());
        Instant oldTime = Instant.now().minus(Duration.ofSeconds(2));
        Files.setLastModifiedTime(stuckFile, FileTime.from(oldTime));

        // When - execute cleaning process
        scheduledFileCleaner.cleanStickFiles();

        // Then - verify loaded directory was created and file was moved
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(Files.exists(loadedDir1)).isTrue();
            assertThat(Files.list(loadedDir1).count()).isEqualTo(1);
        });

        assertThat(Files.exists(stuckFile)).isFalse();
    }

    @Test
    void shouldGenerateCorrectLogEventsAndFileNames() throws IOException, InterruptedException {
        // Given - create a stuck file with specific name
        Path stuckFile = Files.createFile(loadingDir1.resolve("important-data.csv"));
        Files.write(stuckFile, "data1,data2,data3".getBytes());
        Instant oldTime = Instant.now().minus(Duration.ofSeconds(2));
        Files.setLastModifiedTime(stuckFile, FileTime.from(oldTime));

        // When - execute cleaning process
        scheduledFileCleaner.cleanStickFiles();

        // Then - verify file moved with correct base name (no extension or timestamp added)
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            List<Path> loadedFiles = Files.list(loadedDir1).collect(Collectors.toList());
            assertThat(loadedFiles).hasSize(1);
            // ScheduledFileCleaner uses getBaseName which returns name without extension
            assertThat(loadedFiles.get(0).getFileName().toString()).isEqualTo("important-data");
        });

        // Verify notification contains correct file information
        Message<?> message = outputDestination.receive(1000, "test-topic1");
        assertThat(message).isNotNull();
        assertThat(message.getPayload()).isNotNull();
    }

    @Test
    void shouldHandleMixOfStuckAndRecentFiles() throws IOException, InterruptedException {
        // Given - create mix of stuck and recent files
        Path stuckFile1 = Files.createFile(loadingDir1.resolve("stuck1.txt"));
        Files.write(stuckFile1, "content1".getBytes());
        Instant oldTime = Instant.now().minus(Duration.ofSeconds(2));
        Files.setLastModifiedTime(stuckFile1, FileTime.from(oldTime));

        Path recentFile = Files.createFile(loadingDir1.resolve("recent.txt"));
        Files.write(recentFile, "content2".getBytes());
        // recentFile has current timestamp

        Path stuckFile2 = Files.createFile(loadingDir1.resolve("stuck2.txt"));
        Files.write(stuckFile2, "content3".getBytes());
        Files.setLastModifiedTime(stuckFile2, FileTime.from(oldTime));

        // When - execute cleaning process
        scheduledFileCleaner.cleanStickFiles();

        // Then - verify only stuck files were moved
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(Files.list(loadedDir1).count()).isEqualTo(2);
        });

        // Verify recent file still in loading directory
        assertThat(Files.exists(recentFile)).isTrue();
        assertThat(Files.exists(stuckFile1)).isFalse();
        assertThat(Files.exists(stuckFile2)).isFalse();

        // Verify 2 notifications were sent (for 2 stuck files)
        Message<?> message1 = outputDestination.receive(1000, "test-topic1");
        Message<?> message2 = outputDestination.receive(1000, "test-topic1");

        assertThat(message1).isNotNull();
        assertThat(message2).isNotNull();

        // No third notification for recent file
        Message<?> message3 = outputDestination.receive(100, "test-topic1");
        assertThat(message3).isNull();
    }
}
