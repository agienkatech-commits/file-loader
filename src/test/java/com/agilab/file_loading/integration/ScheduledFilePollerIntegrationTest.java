package com.agilab.file_loading.integration;


import com.agilab.file_loading.ScheduledFilePoller;
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
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"test-topic1"},
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:9092",
                "port=9092"
        }
)
class ScheduledFilePollerIntegrationTest {

    @Autowired
    private ScheduledFilePoller scheduledFilePoller;

    @Autowired
    private FileLoaderProperties properties;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @MockitoSpyBean
    private FileNotificationProducer fileNotificationProducer;

    @MockitoSpyBean
    private FileOperations fileOperations;

    private Consumer<String, String> consumer;
    private ObjectMapper objectMapper;

    @TempDir
    Path tempDir;

    private Path baseDir;
    private Path newDir;
    private Path loadedDir;

    @BeforeEach
    void setUp() throws IOException {
        baseDir = tempDir.resolve("test-base");
        newDir = baseDir.resolve(properties.getNewSubdirectory());
        loadedDir = baseDir.resolve(properties.getLoadedSubdirectory());

        Files.createDirectories(newDir);
        Files.createDirectories(loadedDir);

        properties.setSourceDirectories(Map.of(baseDir.toString(), "fileNotification1-out-0"));

        // Setup ObjectMapper with JavaTimeModule for Instant serialization
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        // Setup Kafka consumer
        var consumerProps = KafkaTestUtils.consumerProps("test-group", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var consumerFactory = new DefaultKafkaConsumerFactory<String, String>(consumerProps);
        consumer = consumerFactory.createConsumer();
        consumer.subscribe(List.of("test-topic1"));
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void pollBlobContainers_shouldProcessNewFilesAndSendNotification() throws IOException {
        // Given: A new file in the "new" directory
        var testFile = newDir.resolve("test-file.txt");
        Files.writeString(testFile, "Test content for polling");

        // When: Trigger the scheduled polling
        scheduledFilePoller.pollBlobContainers();

        // Then: File should be processed and moved to loaded directory
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(Files.exists(testFile)).isFalse();
                    assertThat(Files.list(loadedDir).count()).isEqualTo(1);
                });

        // And: Notification should be sent to Kafka with correct event structure
        var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(5));
        assertThat(records.count()).isGreaterThan(0);

        var record = records.iterator().next();
        var event = objectMapper.readValue(record.value(), FileLoadedEvent.class);

        assertThat(event.originalFilePath()).contains("test-file.txt");
        assertThat(event.loadedFilePath()).contains("test-file");
        assertThat(event.baseDirectory()).isEqualTo(baseDir.toString());
        assertThat(event.fileName()).startsWith("test-file-");
        assertThat(event.fileName()).endsWith(".txt");
        assertThat(event.timestamp()).isNotNull();
        assertThat(event.metadata()).isNotNull();
    }

    @Test
    void pollBlobContainers_shouldHandleMultipleFilesInSinglePoll() throws IOException {
        // Given: Multiple files in the "new" directory
        var file1 = newDir.resolve("file1.txt");
        var file2 = newDir.resolve("file2.txt");
        var file3 = newDir.resolve("file3.txt");

        Files.writeString(file1, "Content 1");
        Files.writeString(file2, "Content 2");
        Files.writeString(file3, "Content 3");

        // When: Trigger the scheduled polling
        scheduledFilePoller.pollBlobContainers();

        // Then: All files should be processed
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(Files.exists(file1)).isFalse();
                    assertThat(Files.exists(file2)).isFalse();
                    assertThat(Files.exists(file3)).isFalse();
                    assertThat(Files.list(loadedDir).count()).isEqualTo(3);
                });

        // And: All notifications should be sent with valid event structures
        var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(5));
        assertThat(records.count()).isEqualTo(3);

        records.forEach(record -> {
            try {
                var event = objectMapper.readValue(record.value(), FileLoadedEvent.class);
                assertThat(event.baseDirectory()).isEqualTo(baseDir.toString());
                assertThat(event.timestamp()).isNotNull();
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize event", e);
            }
        });
    }

    @Test
    void pollBlobContainers_shouldHandleEmptyNewDirectory() {
        // Given: Empty "new" directory (no files)

        // When: Trigger the scheduled polling
        scheduledFilePoller.pollBlobContainers();

        // Then: No errors should occur
        assertThat(Files.exists(newDir)).isTrue();

        // And: No notifications should be sent
        var records = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(500));
        assertThat(records.isEmpty()).isTrue();
    }

    @Test
    void pollBlobContainers_shouldSkipTemporaryFiles() throws IOException {
        // Given: Files including temporary files
        var regularFile = newDir.resolve("regular.txt");
        var tempFile1 = newDir.resolve("file.tmp");
        var tempFile2 = newDir.resolve(".temp_file.txt");

        Files.writeString(regularFile, "Regular content");
        Files.writeString(tempFile1, "Temp content 1");
        Files.writeString(tempFile2, "Temp content 2");

        // When: Trigger the scheduled polling
        scheduledFilePoller.pollBlobContainers();

        // Then: Only regular file should be processed
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(Files.exists(regularFile)).isFalse();
                    assertThat(Files.list(loadedDir).count()).isEqualTo(1);
                });

        // And: Temporary files should still exist
        assertThat(Files.exists(tempFile1)).isTrue();
        assertThat(Files.exists(tempFile2)).isTrue();

        // And: Only one notification should be sent
        var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(2));
        assertThat(records.count()).isEqualTo(1);

        var event = objectMapper.readValue(records.iterator().next().value(), FileLoadedEvent.class);
        assertThat(event.fileName()).contains("regular");
    }

    @Test
    void pollBlobContainers_shouldContinueProcessingAfterException() throws Exception {
        // Given: Valid files and a spy that will throw exception on first file move
        var file1 = newDir.resolve("file1.txt");
        var file2 = newDir.resolve("file2.txt");

        Files.writeString(file1, "Content 1");
        Files.writeString(file2, "Content 2");

        // Mock exception on first file operation, then proceed normally
        doThrow(new RuntimeException(new IOException("Simulated IO error")))
                .doCallRealMethod()
                .when(fileOperations).moveFileAtomicallyWithRetry(any(Path.class), any(Path.class));

        // When: Trigger polling (first file should fail, second should succeed)
        scheduledFilePoller.pollBlobContainers();

        // Then: At least one file should still be processed successfully
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    var loadedCount = Files.list(loadedDir).count();
                    assertThat(loadedCount).isGreaterThanOrEqualTo(1);
                });

        // And: At least one notification should be sent
        var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(3));
        assertThat(records.count()).isGreaterThanOrEqualTo(1);

        var event = objectMapper.readValue(records.iterator().next().value(), FileLoadedEvent.class);
        assertThat(event.baseDirectory()).isEqualTo(baseDir.toString());
    }

    @Test
    void pollBlobContainers_shouldProcessFilesWithCorrectKafkaMessageFormat() throws IOException {
        // Given: A test file with specific content
        var testFile = newDir.resolve("format-test.csv");
        Files.writeString(testFile, "col1,col2,col3\nval1,val2,val3");

        // When: Trigger polling
        scheduledFilePoller.pollBlobContainers();

        // Then: Kafka message should have correct FileLoadedEvent structure
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(Files.list(loadedDir).count()).isEqualTo(1));

        var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(5));
        assertThat(records.count()).isGreaterThan(0);

        var firstRecord = records.iterator().next();
        var event = objectMapper.readValue(firstRecord.value(), FileLoadedEvent.class);

        // Verify all FileLoadedEvent fields
        assertThat(event.originalFilePath()).endsWith("format-test.csv");
        assertThat(event.loadedFilePath()).contains("format-test-");
        assertThat(event.loadedFilePath()).endsWith(".csv");
        assertThat(event.baseDirectory()).isEqualTo(baseDir.toString());
        assertThat(event.fileName()).startsWith("format-test-");
        assertThat(event.fileName()).endsWith(".csv");
        assertThat(event.timestamp()).isNotNull();
        assertThat(event.timestamp()).isBefore(java.time.Instant.now());
        assertThat(event.metadata()).isNotNull().isEmpty();
    }

    @Test
    void pollBlobContainers_shouldMoveFileBackWhenNotificationFails() throws IOException {
        // Given
        var testFile = newDir.resolve("fail-notify.txt");
        Files.writeString(testFile, "content causing notification failure");

        doReturn(false).when(fileNotificationProducer).sendFileNotification(any(FileLoadedEvent.class));

        // When
        scheduledFilePoller.pollBlobContainers();

        // Then
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(Files.exists(testFile)).isTrue();
            assertThat(Files.list(loadedDir).count()).isEqualTo(0);
        });

        var records = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(500));
        assertThat(records.isEmpty()).isTrue();
    }

    @Test
    void pollBlobContainers_shouldContinueProcessingOtherFilesWhenNotificationFailsForOne() throws IOException {
        var file1 = newDir.resolve("file1.txt");
        var file2 = newDir.resolve("file2.txt");
        Files.writeString(file1, "content1");
        Files.writeString(file2, "content2");

        doReturn(false).doCallRealMethod().when(fileNotificationProducer).sendFileNotification(any());

        // When
        scheduledFilePoller.pollBlobContainers();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(Files.exists(file1)).isTrue();
            assertThat(Files.exists(file2)).isFalse();
            assertThat(Files.list(loadedDir).count()).isEqualTo(1);
        });

        var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(5));
        assertThat(records.count()).isEqualTo(1);

        var event = objectMapper.readValue(records.iterator().next().value(), FileLoadedEvent.class);
        assertThat(event.fileName()).startsWith("file2");
    }

    @Test
    void pollBlobContainers_shouldMoveFileBackWhenNotificationThrowsException() throws IOException {
        // Given
        var testFile = newDir.resolve("exception-notify.txt");
        Files.writeString(testFile, "content causing notification exception");

        doThrow(new RuntimeException("Simulated sending error")).when(fileNotificationProducer).sendFileNotification(any(FileLoadedEvent.class));

        // When
        scheduledFilePoller.pollBlobContainers();

        // Then
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(Files.exists(testFile)).isTrue();
            assertThat(Files.list(loadedDir).count()).isEqualTo(0);
        });

        var records = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(500));
        assertThat(records.isEmpty()).isTrue();
    }

    @Test
    void pollBlobContainers_shouldContinueProcessingOtherFilesWhenNotificationThrowsForOne() throws IOException {
        var file1 = newDir.resolve("file1.txt");
        var file2 = newDir.resolve("file2.txt");
        Files.writeString(file1, "content1");
        Files.writeString(file2, "content2");

        doThrow(new RuntimeException("Notification service failure"))
                .doCallRealMethod()
                .when(fileNotificationProducer).sendFileNotification(any());

        // When
        scheduledFilePoller.pollBlobContainers();

        // Then
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(Files.exists(file1)).isTrue();
            assertThat(Files.exists(file2)).isFalse();
            assertThat(Files.list(loadedDir).count()).isEqualTo(1);
        });

        var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(3));
        assertThat(records.count()).isEqualTo(1);

        var event = objectMapper.readValue(records.iterator().next().value(), FileLoadedEvent.class);
        assertThat(event.baseDirectory()).isEqualTo(baseDir.toString());
    }
}