package com.agilab.file_loading;

import com.agilab.file_loading.config.FileLoaderProperties;
import com.agilab.file_loading.event.FileLoadedEvent;
import com.agilab.file_loading.util.FilesOperations;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;

import java.util.Base64;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Integration test for ScheduledFileCleaner with embedded Kafka.
 * Tests that stuck files are cleaned up, notifications are sent to Kafka, and files are moved.
 */
@SpringBootTest
@EmbeddedKafka(topics = {"test-stuck-files-topic"}, partitions = 1, 
               brokerProperties = {"listeners=PLAINTEXT://localhost:9093", "port=9093"})
@TestPropertySource(properties = {
        "spring.cloud.stream.kafka.binder.brokers=${spring.embedded.kafka.brokers}",
        "spring.cloud.stream.default-binder=kafka",
        "spring.cloud.stream.bindings.fileNotification1-out-0.destination=test-stuck-files-topic",
        "spring.cloud.stream.bindings.fileNotification1-out-0.content-type=application/json",
        "spring.cloud.stream.kafka.default.producer.configuration.key.serializer=org.apache.kafka.common.serialization.StringSerializer",
        "spring.cloud.stream.kafka.default.producer.configuration.value.serializer=org.springframework.kafka.support.serializer.JsonSerializer",
        "file-loader.new-subdirectory=new",
        "file-loader.loading-subdirectory=loading",
        "file-loader.loaded-subdirectory=loaded",
        "file-loader.retry-attempts=3",
        "file-loader.retry-delay=PT0.1S",
        "file-loader.stuck-file-threshold=PT0.5S",
        "spring.task.scheduling.enabled=false"
})
class ScheduledFileCleanerEmbeddedKafkaIT {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private ScheduledFileCleaner scheduledFileCleaner;

    @Autowired
    private FileLoaderProperties properties;

    @SpyBean
    private FilesOperations filesOperations;

    @TempDir
    Path tempDir;

    private Path baseDir;
    private Path loadingDir;
    private Path loadedDir;
    private Consumer<String, byte[]> consumer;
    private ObjectMapper objectMapper;



    @BeforeEach
    void setUp() throws IOException {
        // Create directory structure
        baseDir = tempDir.resolve("test-base");
        loadingDir = baseDir.resolve("loading");
        loadedDir = baseDir.resolve("loaded");
        var newDir = baseDir.resolve("new");

        Files.createDirectories(newDir);
        Files.createDirectories(loadingDir);
        Files.createDirectories(loadedDir);

        // Configure properties with test directories
        var sourceDirectories = Map.of(baseDir.toString(), "fileNotification1-out-0");
        properties.setSourceDirectories(sourceDirectories);
        properties.setStuckFileThreshold(Duration.ofMillis(500));

        // Set up Kafka consumer with byte array deserializer
        var consumerProps = KafkaTestUtils.consumerProps("test-group", "true", embeddedKafka);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        var consumerFactory = new DefaultKafkaConsumerFactory<String, byte[]>(consumerProps);
        consumer = consumerFactory.createConsumer();
        consumer.subscribe(Collections.singletonList("test-stuck-files-topic"));
        
        // Initialize ObjectMapper for manual deserialization
        objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();

        // Mock FilesOperations to actually perform the move
        doAnswer(invocation -> {
            Path source = invocation.getArgument(0);
            Path target = invocation.getArgument(1);
            if (Files.exists(source)) {
                Files.createDirectories(target.getParent());
                Files.move(source, target);
            }
            return null;
        }).when(filesOperations).moveFileAtomicallyWithRetry(any(Path.class), any(Path.class));
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void shouldCleanStuckFilesAndSendKafkaNotification() throws IOException, InterruptedException {
        // Given - create a stuck file in loading directory
        var stuckFileName = "stuck-file.txt";
        var stuckFile = loadingDir.resolve(stuckFileName);
        Files.write(stuckFile, "stuck content".getBytes());

        // Set the file's last modified time to more than stuckFileThreshold ago
        var oldTime = Instant.now().minus(Duration.ofSeconds(2));
        Files.setLastModifiedTime(stuckFile, java.nio.file.attribute.FileTime.from(oldTime));

        // Verify the file is old enough
        var fileAge = Duration.between(
            Files.getLastModifiedTime(stuckFile).toInstant(),
            Instant.now()
        );
        assertThat(fileAge).isGreaterThan(properties.getStuckFileThreshold());

        // When - clean stuck files
        scheduledFileCleaner.cleanStickFiles();

        // Give Kafka time to deliver the message
        Thread.sleep(1000);

        // Then - verify FilesOperations.moveFileAtomicallyWithRetry was called
        verify(filesOperations, times(1)).moveFileAtomicallyWithRetry(any(Path.class), any(Path.class));

        // Verify the file was physically moved to loaded directory
        assertThat(stuckFile).doesNotExist();
        var loadedFiles = Files.list(loadedDir).toList();
        assertThat(loadedFiles).hasSize(1);
        assertThat(loadedFiles.get(0).getFileName().toString()).startsWith("stuck-file");

        // Verify Kafka message was sent
        ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(5));
        assertThat(records.isEmpty()).isFalse();

        FileLoadedEvent event = null;
        for (ConsumerRecord<String, byte[]> record : records) {
            // Convert bytes to string first, then check if it's a Base64-wrapped JSON
            var messageStr = new String(record.value());
            if (messageStr.startsWith("\"") && messageStr.endsWith("\"")) {
                // It's a JSON string, unwrap it
                messageStr = objectMapper.readValue(messageStr, String.class);
                // Now decode from Base64
                var jsonBytes = Base64.getDecoder().decode(messageStr);
                event = objectMapper.readValue(jsonBytes, FileLoadedEvent.class);
            } else {
                // It's direct JSON
                event = objectMapper.readValue(record.value(), FileLoadedEvent.class);
            }
            break;
        }

        assertThat(event).isNotNull();
        assertThat(event.originalFilePath()).endsWith(stuckFileName);
        assertThat(event.baseDirectory()).isEqualTo(baseDir.toString());
        assertThat(event.fileName()).startsWith("stuck-file");
        assertThat(event.metadata()).isEqualTo(Map.of());
    }

    @Test
    void shouldNotCleanFilesNewerThanThreshold() throws IOException, InterruptedException {
        // Given - create a recent file in loading directory
        var recentFileName = "recent-file.txt";
        var recentFile = loadingDir.resolve(recentFileName);
        Files.write(recentFile, "recent content".getBytes());

        // File is recent (default last modified time is now)
        var fileAge = Duration.between(
            Files.getLastModifiedTime(recentFile).toInstant(),
            Instant.now()
        );
        assertThat(fileAge).isLessThan(properties.getStuckFileThreshold());

        // When - clean stuck files
        scheduledFileCleaner.cleanStickFiles();

        // Give time for potential processing
        Thread.sleep(500);

        // Then - verify FilesOperations.moveFileAtomicallyWithRetry was NOT called
        verify(filesOperations, never()).moveFileAtomicallyWithRetry(any(Path.class), any(Path.class));

        // Verify the file was NOT moved
        assertThat(recentFile).exists();
        assertThat(Files.list(loadedDir).count()).isEqualTo(0);

        // Verify no Kafka message was sent
        ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
        assertThat(records.isEmpty()).isTrue();
    }

    @Test
    void shouldHandleMultipleStuckFiles() throws IOException, InterruptedException {
        // Given - create multiple stuck files in loading directory
        var stuckFile1 = loadingDir.resolve("stuck1.txt");
        var stuckFile2 = loadingDir.resolve("stuck2.txt");
        Files.write(stuckFile1, "content1".getBytes());
        Files.write(stuckFile2, "content2".getBytes());

        // Set files' last modified time to be old
        var oldTime = Instant.now().minus(Duration.ofSeconds(2));
        Files.setLastModifiedTime(stuckFile1, java.nio.file.attribute.FileTime.from(oldTime));
        Files.setLastModifiedTime(stuckFile2, java.nio.file.attribute.FileTime.from(oldTime));

        // When - clean stuck files
        scheduledFileCleaner.cleanStickFiles();

        // Give Kafka time to deliver messages
        Thread.sleep(1000);

        // Then - verify both files were processed
        verify(filesOperations, times(2)).moveFileAtomicallyWithRetry(any(Path.class), any(Path.class));

        // Verify both files were moved
        assertThat(stuckFile1).doesNotExist();
        assertThat(stuckFile2).doesNotExist();
        assertThat(Files.list(loadedDir).count()).isEqualTo(2);

        // Verify Kafka messages were sent for both files
        ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(5));
        assertThat(records.count()).isGreaterThanOrEqualTo(2);
    }
}
