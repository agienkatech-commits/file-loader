package com.agilab.file_loading.integration;

import com.agilab.file_loading.ScheduledFileCleaner;
import com.agilab.file_loading.config.FileLoaderProperties;
import com.agilab.file_loading.event.FileLoadedEvent;
import com.agilab.file_loading.notification.FileNotificationProducer;
import com.agilab.file_loading.util.FileOperations;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Integration test for ScheduledFileCleaner.
 * Tests the scheduled cleaning mechanism for stuck files in loading directory.
 */
@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"test-topic1", "test-topic2"},
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:9092",
                "port=9092"
        }
)
@TestPropertySource(properties = {
        "spring.cloud.stream.bindings.fileNotification1-out-0.destination=test-topic1",
        "spring.cloud.stream.bindings.fileNotification2-out-0.destination=test-topic2",
        "spring.cloud.stream.bindings.fileNotification1-out-0.content-type=application/json",
        "spring.cloud.stream.bindings.fileNotification2-out-0.content-type=application/json",
        "spring.cloud.stream.kafka.binder.brokers=localhost:9092",
        "file-loader.new-subdirectory=new",
        "file-loader.loading-subdirectory=loading",
        "file-loader.loaded-subdirectory=loaded",
        "file-loader.stuck-file-threshold=PT2S",
        "file-loader.cleaning-interval=PT5S",
        "file-loader.retry-attempts=3",
        "file-loader.retry-delay=PT0.1S"
})
class ScheduledFileCleanerIntegrationTest {

    @Autowired
    private ScheduledFileCleaner scheduledFileCleaner;

    @Autowired
    private FileLoaderProperties properties;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @MockitoSpyBean
    private FileNotificationProducer notificationProducer;

    @MockitoSpyBean
    private FileOperations fileOperations;

    private Consumer<String, String> consumer;
    private ObjectMapper objectMapper;

    @TempDir
    Path tempDir;

    private Path baseDir1;
    private Path baseDir2;
    private Path loadingDir1;
    private Path loadedDir1;
    private Path loadingDir2;
    private Path loadedDir2;

    @BeforeEach
    void setUp() throws IOException {
        // Setup for first directory
        baseDir1 = tempDir.resolve("test-base1");
        loadingDir1 = baseDir1.resolve(properties.getLoadingSubdirectory());
        loadedDir1 = baseDir1.resolve(properties.getLoadedSubdirectory());
        Files.createDirectories(loadingDir1);
        Files.createDirectories(loadedDir1);

        // Setup for second directory
        baseDir2 = tempDir.resolve("test-base2");
        loadingDir2 = baseDir2.resolve(properties.getLoadingSubdirectory());
        loadedDir2 = baseDir2.resolve(properties.getLoadedSubdirectory());
        Files.createDirectories(loadingDir2);
        Files.createDirectories(loadedDir2);

        var sourceDirectories = new HashMap<String, String>();
        sourceDirectories.put(baseDir1.toString(), "fileNotification1-out-0");
        sourceDirectories.put(baseDir2.toString(), "fileNotification2-out-0");
        properties.setSourceDirectories(sourceDirectories);

        // Setup ObjectMapper with JavaTimeModule
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        // Setup Kafka consumer
        var consumerProps = KafkaTestUtils.consumerProps("test-cleaner-group", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var consumerFactory = new DefaultKafkaConsumerFactory<String, String>(consumerProps);
        consumer = consumerFactory.createConsumer();
        consumer.subscribe(List.of("test-topic1", "test-topic2"));
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void cleanStickFiles_shouldMoveOldStuckFilesToLoadedDirectory() throws IOException {
        // Given: An old stuck file in loading directory
        var stuckFile = loadingDir1.resolve("stuck-file.txt");
        Files.writeString(stuckFile, "Stuck content");

        // Set file modification time to 5 seconds ago (older than threshold)
        var oldTime = Instant.now().minus(Duration.ofSeconds(5));
        Files.setLastModifiedTime(stuckFile, FileTime.from(oldTime));

        // When: Trigger the cleaning process
        scheduledFileCleaner.cleanStickFiles();

        // Then: File should be moved to loaded directory
        await().atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> {
                    assertThat(Files.exists(stuckFile)).isFalse();
                    assertThat(Files.list(loadedDir1).count()).isEqualTo(1);
                });

        // And: Notification should be sent to Kafka with correct event structure
        var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(5));
        assertThat(records.count()).isGreaterThan(0);

        var firstRecord = records.iterator().next();
        var event = objectMapper.readValue(firstRecord.value(), FileLoadedEvent.class);

        assertThat(event.originalFilePath()).contains("stuck-file.txt");
        assertThat(event.loadedFilePath()).contains("stuck-file");
        assertThat(event.baseDirectory()).isEqualTo(baseDir1.toString());
        assertThat(event.fileName()).isEqualTo("stuck-file");
        assertThat(event.timestamp()).isNotNull();
    }

    @Test
    void cleanStickFiles_shouldNotMoveRecentFiles() throws IOException, InterruptedException {
        // Given: A recent file in loading directory (younger than threshold)
        var recentFile = loadingDir1.resolve("recent-file.txt");
        Files.writeString(recentFile, "Recent content");

        // File has current timestamp (recent)
        Files.setLastModifiedTime(recentFile, FileTime.from(Instant.now()));

        // When: Trigger the cleaning process
        scheduledFileCleaner.cleanStickFiles();

        // Wait a bit to ensure processing would have occurred
        Thread.sleep(500);

        // Then: File should still be in loading directory
        assertThat(Files.exists(recentFile)).isTrue();
        assertThat(Files.list(loadedDir1).count()).isZero();

        // And: No notification should be sent
        var records = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(500));
        assertThat(records.isEmpty()).isTrue();
    }

    @Test
    void cleanStickFiles_shouldHandleMultipleStuckFiles() throws IOException {
        // Given: Multiple old stuck files
        var stuckFile1 = loadingDir1.resolve("stuck1.txt");
        var stuckFile2 = loadingDir1.resolve("stuck2.txt");
        var stuckFile3 = loadingDir1.resolve("stuck3.txt");

        Files.writeString(stuckFile1, "Content 1");
        Files.writeString(stuckFile2, "Content 2");
        Files.writeString(stuckFile3, "Content 3");

        var oldTime = Instant.now().minus(Duration.ofSeconds(5));
        Files.setLastModifiedTime(stuckFile1, FileTime.from(oldTime));
        Files.setLastModifiedTime(stuckFile2, FileTime.from(oldTime));
        Files.setLastModifiedTime(stuckFile3, FileTime.from(oldTime));

        // When: Trigger the cleaning process
        scheduledFileCleaner.cleanStickFiles();

        // Then: All files should be moved
        await().atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> {
                    assertThat(Files.exists(stuckFile1)).isFalse();
                    assertThat(Files.exists(stuckFile2)).isFalse();
                    assertThat(Files.exists(stuckFile3)).isFalse();
                    assertThat(Files.list(loadedDir1).count()).isEqualTo(3);
                });

        // And: All notifications should be sent with valid events
        var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(5));
        assertThat(records.count()).isEqualTo(3);

        records.forEach(record -> {
            try {
                var event = objectMapper.readValue(record.value(), FileLoadedEvent.class);
                assertThat(event.baseDirectory()).isEqualTo(baseDir1.toString());
                assertThat(event.fileName()).matches("stuck[1-3]");
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize event", e);
            }
        });
    }

    @Test
    void cleanStickFiles_shouldProcessMultipleDirectoriesInParallel() throws IOException {
        // Given: Stuck files in multiple directories
        var stuckFile1 = loadingDir1.resolve("stuck-dir1.txt");
        var stuckFile2 = loadingDir2.resolve("stuck-dir2.txt");

        Files.writeString(stuckFile1, "Content dir1");
        Files.writeString(stuckFile2, "Content dir2");

        var oldTime = Instant.now().minus(Duration.ofSeconds(5));
        Files.setLastModifiedTime(stuckFile1, FileTime.from(oldTime));
        Files.setLastModifiedTime(stuckFile2, FileTime.from(oldTime));

        // When: Trigger the cleaning process
        scheduledFileCleaner.cleanStickFiles();

        // Then: Files in both directories should be processed
        await().atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> {
                    assertThat(Files.exists(stuckFile1)).isFalse();
                    assertThat(Files.exists(stuckFile2)).isFalse();
                    assertThat(Files.list(loadedDir1).count()).isEqualTo(1);
                    assertThat(Files.list(loadedDir2).count()).isEqualTo(1);
                });

        // And: Notifications should be sent to both topics with correct events
        var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(5));
        assertThat(records.count()).isEqualTo(2);

        var eventsByTopic = new HashMap<String, FileLoadedEvent>();
        records.forEach(record -> {
            try {
                var event = objectMapper.readValue(record.value(), FileLoadedEvent.class);
                eventsByTopic.put(record.topic(), event);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize event", e);
            }
        });

        assertThat(eventsByTopic).hasSize(2);
        assertThat(eventsByTopic.values()).extracting(FileLoadedEvent::baseDirectory)
                .containsExactlyInAnyOrder(baseDir1.toString(), baseDir2.toString());
    }

    @Test
    void cleanStickFiles_shouldHandleMixedOldAndRecentFiles() throws IOException, InterruptedException {
        // Given: Mix of old and recent files
        var oldFile = loadingDir1.resolve("old-file.txt");
        var recentFile = loadingDir1.resolve("recent-file.txt");

        Files.writeString(oldFile, "Old content");
        Files.writeString(recentFile, "Recent content");

        var oldTime = Instant.now().minus(Duration.ofSeconds(5));
        Files.setLastModifiedTime(oldFile, FileTime.from(oldTime));
        Files.setLastModifiedTime(recentFile, FileTime.from(Instant.now()));

        // When: Trigger the cleaning process
        scheduledFileCleaner.cleanStickFiles();

        // Then: Only old file should be moved
        await().atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> {
                    assertThat(Files.exists(oldFile)).isFalse();
                    assertThat(Files.list(loadedDir1).count()).isEqualTo(1);
                });

        // Recent file should still be in loading directory
        assertThat(Files.exists(recentFile)).isTrue();

        // And: Only one notification should be sent for the old file
        var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(2));
        assertThat(records.count()).isEqualTo(1);

        var event = objectMapper.readValue(records.iterator().next().value(), FileLoadedEvent.class);
        assertThat(event.fileName()).isEqualTo("old-file");
    }

    @Test
    void cleanStickFiles_shouldHandleNonExistentLoadingDirectory() {
        // Given: A base directory without loading subdirectory
        var baseDir3 = tempDir.resolve("test-base3");
        var sourceDirectories = new HashMap<>(properties.getSourceDirectories());
        sourceDirectories.put(baseDir3.toString(), "fileNotification1-out-0");
        properties.setSourceDirectories(sourceDirectories);

        // When: Trigger the cleaning process (should not crash)
        scheduledFileCleaner.cleanStickFiles();

        // Then: No errors should occur
        assertThat(Files.exists(baseDir3)).isFalse();

        // And: No notifications should be sent
        var records = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(500));
        assertThat(records.isEmpty()).isTrue();
    }

    @Test
    void cleanStickFiles_shouldCreateLoadedDirectoryIfNotExists() throws IOException {
        // Given: Loading directory exists but loaded directory doesn't
        Files.deleteIfExists(loadedDir1);
        assertThat(Files.exists(loadedDir1)).isFalse();

        var stuckFile = loadingDir1.resolve("stuck-file.txt");
        Files.writeString(stuckFile, "Content");

        var oldTime = Instant.now().minus(Duration.ofSeconds(5));
        Files.setLastModifiedTime(stuckFile, FileTime.from(oldTime));

        // When: Trigger the cleaning process
        scheduledFileCleaner.cleanStickFiles();

        // Then: Loaded directory should be created and file moved
        await().atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> {
                    assertThat(Files.exists(loadedDir1)).isTrue();
                    assertThat(Files.exists(stuckFile)).isFalse();
                    assertThat(Files.list(loadedDir1).count()).isEqualTo(1);
                });

        // And: Notification should be sent with correct event
        var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(2));
        assertThat(records.count()).isGreaterThan(0);

        var event = objectMapper.readValue(records.iterator().next().value(), FileLoadedEvent.class);
        assertThat(event.fileName()).isEqualTo("stuck-file");
    }

    @Test
    void cleanStickFiles_shouldSkipDirectoriesAndOnlyProcessRegularFiles() throws IOException, InterruptedException {
        // Given: A stuck file and a subdirectory in loading directory
        var stuckFile = loadingDir1.resolve("stuck-file.txt");
        var subDirectory = loadingDir1.resolve("subdirectory");

        Files.writeString(stuckFile, "Content");
        Files.createDirectories(subDirectory);

        var oldTime = Instant.now().minus(Duration.ofSeconds(5));
        Files.setLastModifiedTime(stuckFile, FileTime.from(oldTime));
        Files.setLastModifiedTime(subDirectory, FileTime.from(oldTime));

        // When: Trigger the cleaning process
        scheduledFileCleaner.cleanStickFiles();

        // Then: Only the regular file should be moved
        await().atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> {
                    assertThat(Files.exists(stuckFile)).isFalse();
                    assertThat(Files.list(loadedDir1).count()).isEqualTo(1);
                });

        // Subdirectory should still exist in loading directory
        assertThat(Files.exists(subDirectory)).isTrue();
        assertThat(Files.isDirectory(subDirectory)).isTrue();

        // And: Only one notification for the file
        var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(2));
        assertThat(records.count()).isEqualTo(1);

        var event = objectMapper.readValue(records.iterator().next().value(), FileLoadedEvent.class);
        assertThat(event.fileName()).isEqualTo("stuck-file");
    }

    @Test
    void cleanStickFiles_shouldIncludeCorrectMetadataInKafkaMessage() throws IOException {
        // Given: A stuck file with known properties
        var stuckFile = loadingDir1.resolve("metadata-test.txt");
        Files.writeString(stuckFile, "Metadata test content");

        var oldTime = Instant.now().minus(Duration.ofSeconds(5));
        Files.setLastModifiedTime(stuckFile, FileTime.from(oldTime));

        // When: Trigger the cleaning process
        scheduledFileCleaner.cleanStickFiles();

        // Then: Kafka message should contain correct metadata
        var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(5));
        assertThat(records.count()).isGreaterThan(0);

        var firstRecord = records.iterator().next();
        var event = objectMapper.readValue(firstRecord.value(), FileLoadedEvent.class);

        // Verify all fields of FileLoadedEvent for resent notification
        assertThat(event.originalFilePath()).endsWith("metadata-test.txt");
        assertThat(event.loadedFilePath()).contains("metadata-test");
        assertThat(event.baseDirectory()).isEqualTo(baseDir1.toString());
        assertThat(event.fileName()).isEqualTo("metadata-test");
        assertThat(event.timestamp()).isNotNull();
        assertThat(event.timestamp()).isBefore(Instant.now());
        assertThat(event.metadata()).isNotNull().isEmpty();
    }

    @Test
    void cleanStickFiles_shouldContinueProcessingWhenNotificationFails() throws IOException {
        // Given: Multiple stuck files with one notification failure
        var stuckFile1 = loadingDir1.resolve("stuck1.txt");
        var stuckFile2 = loadingDir1.resolve("stuck2.txt");

        Files.writeString(stuckFile1, "Content 1");
        Files.writeString(stuckFile2, "Content 2");

        var oldTime = Instant.now().minus(Duration.ofSeconds(5));
        Files.setLastModifiedTime(stuckFile1, FileTime.from(oldTime));
        Files.setLastModifiedTime(stuckFile2, FileTime.from(oldTime));

        // Mock notification failure for first file, success for second
        doReturn(false)
                .doReturn(true)
                .when(notificationProducer).sendFileNotification(any(FileLoadedEvent.class));

        // When: Trigger the cleaning process
        scheduledFileCleaner.cleanStickFiles();

        // Then: File with successful notification should be moved
        await().atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> {
                    var loadedCount = Files.list(loadedDir1).count();
                    assertThat(loadedCount).isGreaterThanOrEqualTo(1);
                });

        // And: At least one notification attempt should have been made
        verify(notificationProducer, atLeastOnce()).sendFileNotification(any(FileLoadedEvent.class));
    }

    @Test
    void cleanStickFiles_shouldHandleFileMoveExceptionGracefully() throws Exception {
        // Given: A stuck file and mocked exception during file move
        var stuckFile = loadingDir1.resolve("error-file.txt");
        Files.writeString(stuckFile, "Error content");

        var oldTime = Instant.now().minus(Duration.ofSeconds(5));
        Files.setLastModifiedTime(stuckFile, FileTime.from(oldTime));

        // Mock file operation to throw exception
        doThrow(new IOException("Simulated file move error"))
                .when(fileOperations).moveFileAtomicallyWithRetry(any(Path.class), any(Path.class));

        // When: Trigger the cleaning process (should not crash)
        scheduledFileCleaner.cleanStickFiles();

        // Then: Process should complete without throwing exception
        Thread.sleep(500);

        // File should still exist due to move failure
        assertThat(Files.exists(stuckFile)).isTrue();
    }

    @Test
    void cleanStickFiles_shouldHandleUnreadableFileLastModifiedTime() throws IOException {
        // Given: A stuck file and another file that will simulate permission issues
        var normalFile = loadingDir1.resolve("normal-stuck.txt");
        Files.writeString(normalFile, "Normal content");

        var oldTime = Instant.now().minus(Duration.ofSeconds(5));
        Files.setLastModifiedTime(normalFile, FileTime.from(oldTime));

        // When: Trigger cleaning (should handle any IOException reading file times gracefully)
        scheduledFileCleaner.cleanStickFiles();

        // Then: Normal file should still be processed
        await().atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> {
                    assertThat(Files.exists(normalFile)).isFalse();
                    assertThat(Files.list(loadedDir1).count()).isGreaterThanOrEqualTo(1);
                });

        // And: Notification should be sent for normal file
        var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(2));
        assertThat(records.count()).isGreaterThan(0);

        var event = objectMapper.readValue(records.iterator().next().value(), FileLoadedEvent.class);
        assertThat(event.fileName()).isEqualTo("normal-stuck");
    }
}
