package com.agilab.file_loading;

import com.agilab.file_loading.config.FileLoaderProperties;
import com.agilab.file_loading.event.FileLoadedEvent;
import com.agilab.file_loading.util.FilesOperations;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.commons.io.FilenameUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

@SpringBootTest(properties = {
        "spring.cloud.stream.kafka.binder.brokers=${spring.embedded.kafka.brokers}",
        "spring.cloud.stream.bindings.fileNotification1-out-0.destination=test-topic",
        "spring.cloud.stream.bindings.fileNotification1-out-0.content-type=application/json"
})
@EmbeddedKafka(topics = "test-topic", partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:0", "port=0"})
class ScheduledFileCleanerEmbeddedKafkaIT {

    private static final String TOPIC = "test-topic";
    
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private ScheduledFileCleaner scheduledFileCleaner;

    @Autowired
    private FileLoaderProperties fileLoaderProperties;

    @MockBean
    private FilesOperations filesOperations;

    private Path baseDir;
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());

    @BeforeEach
    void setUp() throws Exception {
        var tmp = Files.createTempDirectory("file-loader-it-");
        baseDir = tmp;
        fileLoaderProperties.getSourceDirectories().clear();
        fileLoaderProperties.getSourceDirectories().put(baseDir.toString(), "fileNotification1-out-0");
        fileLoaderProperties.setLoadingSubdirectory("loading");
        fileLoaderProperties.setLoadedSubdirectory("loaded");
        fileLoaderProperties.setStuckFileThreshold(Duration.ofSeconds(5));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (baseDir != null && Files.exists(baseDir)) {
            try (var stream = Files.walk(baseDir)) {
                stream.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (Exception ignored) {}
                });
            }
        }
    }

    @Test
    void whenStuckFileExists_thenCleanerSendsNotificationToKafkaAndMovesFile() throws Exception {
        var loadingDir = baseDir.resolve(fileLoaderProperties.getLoadingSubdirectory());
        var loadedDir = baseDir.resolve(fileLoaderProperties.getLoadedSubdirectory());
        Files.createDirectories(loadingDir);
        Files.createDirectories(loadedDir);

        var fileName = "stuck-file.txt";
        var stuckFile = loadingDir.resolve(fileName);
        Files.writeString(stuckFile, "payload");

        var past = FileTime.from(Instant.now().minus(fileLoaderProperties.getStuckFileThreshold()).minus(Duration.ofMinutes(1)));
        Files.setLastModifiedTime(stuckFile, past);

        // when the cleaner asks FilesOperations to move, perform a real Files.move so we can assert physical movement
        doAnswer(invocation -> {
            var src = invocation.getArgument(0, Path.class);
            var dst = invocation.getArgument(1, Path.class);
            Files.createDirectories(dst.getParent());
            Files.move(src, dst, StandardCopyOption.REPLACE_EXISTING);
            return null;
        }).when(filesOperations).moveFileAtomicallyWithRetry(any(Path.class), any(Path.class));

        // consumer properties (use var + Map.of for additions)
        var consumerProps = KafkaTestUtils.consumerProps("it-group", "true", embeddedKafkaBroker);
        consumerProps.putAll(Map.of(
                "key.deserializer", StringDeserializer.class,
                "value.deserializer", StringDeserializer.class,
                "auto.offset.reset", "earliest"
        ));

        Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<String, String>(consumerProps).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, TOPIC);

        try (consumer) {
            // act
            scheduledFileCleaner.cleanStickFiles();

            // wait for records (timeout builtin)
            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(10_000));
            Assertions.assertThat(records.count()).isGreaterThanOrEqualTo(1);

            var rec = records.iterator().next();
            var payloadJson = rec.value();
            var event = objectMapper.readValue(payloadJson, FileLoadedEvent.class);

            Assertions.assertThat(event.baseDirectory()).isEqualTo(baseDir.toString());
            Assertions.assertThat(event.loadedFilePath()).contains(fileLoaderProperties.getLoadedSubdirectory());
            Assertions.assertThat(event.originalFilePath()).contains(loadingDir.toString());

            var expectedLoadedName = FilenameUtils.getBaseName(stuckFile.toString());
            var expectedLoadedPath = loadedDir.resolve(expectedLoadedName);
            verify(filesOperations, times(1)).moveFileAtomicallyWithRetry(any(Path.class), any(Path.class));
            Assertions.assertThat(Files.exists(expectedLoadedPath)).isTrue();
            Assertions.assertThat(Files.exists(stuckFile)).isFalse();
        }
    }
}
