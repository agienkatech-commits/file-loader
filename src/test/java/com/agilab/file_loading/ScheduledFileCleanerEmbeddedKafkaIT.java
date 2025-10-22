package com.agilab.file_loading;

import com.agilab.file_loading.config.FileLoaderProperties;
import com.agilab.file_loading.event.FileLoadedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for ScheduledFileCleaner using embedded Kafka broker.
 * Tests that stuck files in loading directory are detected, moved to loaded directory,
 * and FileLoadedEvent is sent to Kafka topic.
 */
@SpringBootTest(classes = FileLoadingApplication.class, properties = {
    "spring.cloud.stream.kafka.binder.brokers=${spring.embedded.kafka.brokers}"
})
@EmbeddedKafka(partitions = 1, topics = {"test-cleaner-topic"})
@TestPropertySource(properties = {
        "spring.cloud.stream.bindings.cleanerNotification-out-0.destination=test-cleaner-topic",
        "spring.cloud.stream.bindings.cleanerNotification-out-0.content-type=application/json",
        "file-loader.new-subdirectory=new",
        "file-loader.loading-subdirectory=loading",
        "file-loader.loaded-subdirectory=loaded",
        "file-loader.retry-attempts=3",
        "file-loader.retry-delay=PT0.1S",
        "file-loader.stuck-file-threshold=PT1S",
        "spring.task.scheduling.enabled=false"
})
class ScheduledFileCleanerEmbeddedKafkaIT {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private ScheduledFileCleaner scheduledFileCleaner;

    @Autowired
    private FileLoaderProperties properties;

    private KafkaMessageListenerContainer<String, String> container;
    private BlockingQueue<ConsumerRecord<String, String>> records;
    private ObjectMapper objectMapper;

    private Path tempDir;
    private Path baseDir;
    private Path loadingDir;
    private Path loadedDir;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        // The @EmbeddedKafka annotation makes the broker address available
        // We don't need to do anything here as it's automatically configured
    }

    @BeforeEach
    void setUp() throws IOException {
        objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
        
        // Create temporary directories
        tempDir = Files.createTempDirectory("cleaner-test-");
        baseDir = tempDir.resolve("test-base");
        Path newDir = baseDir.resolve(properties.getNewSubdirectory());
        loadingDir = baseDir.resolve(properties.getLoadingSubdirectory());
        loadedDir = baseDir.resolve(properties.getLoadedSubdirectory());

        Files.createDirectories(newDir);
        Files.createDirectories(loadingDir);
        Files.createDirectories(loadedDir);

        // Configure the test directory
        Map<String, String> sourceDirectories = new HashMap<>();
        sourceDirectories.put(baseDir.toString(), "cleanerNotification-out-0");
        properties.setSourceDirectories(sourceDirectories);

        // Set up Kafka consumer
        setupKafkaConsumer();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (container != null) {
            container.stop();
        }
        // Clean up temporary directories
        if (tempDir != null && Files.exists(tempDir)) {
            Files.walk(tempDir)
                .sorted((a, b) -> b.compareTo(a)) // Delete files before directories
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        // Ignore cleanup errors
                    }
                });
        }
    }

    @Test
    void shouldCleanStuckFileAndSendEventToKafka() throws Exception {
        // Given - create a stuck file in loading directory
        Path stuckFile = loadingDir.resolve("stuck-file.txt");
        Files.write(stuckFile, "stuck file content".getBytes());
        
        // Set the file's last modified time to be older than the threshold
        Instant oldTime = Instant.now().minus(5, ChronoUnit.MINUTES);
        Files.setLastModifiedTime(stuckFile, FileTime.from(oldTime));
        
        assertThat(stuckFile).exists();
        assertThat(Files.list(loadingDir).count()).isEqualTo(1);
        assertThat(Files.list(loadedDir).count()).isEqualTo(0);

        // When - invoke the cleaning process
        scheduledFileCleaner.cleanStickFiles();

        // Then - verify file was moved to loaded directory
        assertThat(stuckFile).doesNotExist();
        assertThat(Files.list(loadingDir).count()).isEqualTo(0);
        assertThat(Files.list(loadedDir).count()).isEqualTo(1);
        
        Path movedFile = Files.list(loadedDir).findFirst().orElseThrow();
        assertThat(movedFile.getFileName().toString()).isEqualTo("stuck-file");
        assertThat(Files.readAllBytes(movedFile)).isEqualTo("stuck file content".getBytes());

        // Verify event was sent to Kafka
        ConsumerRecord<String, String> record = records.poll(10, TimeUnit.SECONDS);
        assertThat(record).isNotNull();
        assertThat(record.topic()).isEqualTo("test-cleaner-topic");
        
        FileLoadedEvent event = objectMapper.readValue(record.value(), FileLoadedEvent.class);
        assertThat(event).isNotNull();
        assertThat(event.originalFilePath()).isEqualTo(stuckFile.toString());
        assertThat(event.loadedFilePath()).isEqualTo(movedFile.toString());
        assertThat(event.baseDirectory()).isEqualTo(baseDir.toString());
        assertThat(event.fileName()).isEqualTo("stuck-file");
    }

    @Test
    void shouldNotCleanRecentFiles() throws Exception {
        // Given - create a recent file in loading directory
        Path recentFile = loadingDir.resolve("recent-file.txt");
        Files.write(recentFile, "recent file content".getBytes());
        
        // File has current timestamp (not stuck)
        assertThat(recentFile).exists();

        // When - invoke the cleaning process
        scheduledFileCleaner.cleanStickFiles();

        // Then - verify file was NOT moved (it's not old enough)
        assertThat(recentFile).exists();
        assertThat(Files.list(loadingDir).count()).isEqualTo(1);
        assertThat(Files.list(loadedDir).count()).isEqualTo(0);

        // Verify no event was sent to Kafka
        ConsumerRecord<String, String> record = records.poll(2, TimeUnit.SECONDS);
        assertThat(record).isNull();
    }

    @Test
    void shouldHandleMultipleStuckFiles() throws Exception {
        // Given - create multiple stuck files
        Path stuckFile1 = loadingDir.resolve("stuck1.txt");
        Path stuckFile2 = loadingDir.resolve("stuck2.txt");
        Files.write(stuckFile1, "content1".getBytes());
        Files.write(stuckFile2, "content2".getBytes());
        
        Instant oldTime = Instant.now().minus(5, ChronoUnit.MINUTES);
        Files.setLastModifiedTime(stuckFile1, FileTime.from(oldTime));
        Files.setLastModifiedTime(stuckFile2, FileTime.from(oldTime));

        // When - invoke the cleaning process
        scheduledFileCleaner.cleanStickFiles();

        // Then - verify both files were moved
        assertThat(Files.list(loadingDir).count()).isEqualTo(0);
        assertThat(Files.list(loadedDir).count()).isEqualTo(2);

        // Verify two events were sent to Kafka
        ConsumerRecord<String, String> record1 = records.poll(10, TimeUnit.SECONDS);
        ConsumerRecord<String, String> record2 = records.poll(10, TimeUnit.SECONDS);
        
        assertThat(record1).isNotNull();
        assertThat(record2).isNotNull();
    }

    @Test
    void shouldHandleNonExistentLoadingDirectory() throws Exception {
        // Given - no loading directory exists for this base directory
        Path nonExistentBase = tempDir.resolve("non-existent-base");
        Map<String, String> sourceDirectories = new HashMap<>();
        sourceDirectories.put(nonExistentBase.toString(), "cleanerNotification-out-0");
        properties.setSourceDirectories(sourceDirectories);

        // When - invoke the cleaning process (should not throw exception)
        scheduledFileCleaner.cleanStickFiles();

        // Then - no errors, no events sent
        ConsumerRecord<String, String> record = records.poll(2, TimeUnit.SECONDS);
        assertThat(record).isNull();
    }

    private void setupKafkaConsumer() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
            "test-consumer-group", "true", embeddedKafka);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        DefaultKafkaConsumerFactory<String, String> consumerFactory = 
            new DefaultKafkaConsumerFactory<>(consumerProps);

        ContainerProperties containerProperties = new ContainerProperties("test-cleaner-topic");
        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
        
        records = new LinkedBlockingQueue<>();
        container.setupMessageListener((MessageListener<String, String>) records::add);
        
        container.start();
        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
    }
}
