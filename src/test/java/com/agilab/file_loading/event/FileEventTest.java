package com.agilab.file_loading.event;

import com.agilab.file_loading.util.FileEventHandler;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for sealed FileEvent interface and pattern matching.
 * Demonstrates Java 25 features.
 */
class FileEventTest {

    private final FileEventHandler eventHandler = new FileEventHandler();

    @Test
    void testFileProcessedEvent_ImplementsFileEvent() {
        var event = FileProcessedEvent.builder()
                .fileName("test.txt")
                .originalFilePath("/mnt/new/test.txt")
                .processedFilePath("/mnt/processed/test-2025.txt")
                .sourceDirectory("/mnt")
                .timestamp(Instant.now())
                .build();

        assertInstanceOf(FileEvent.class, event);
        assertEquals("test.txt", event.getFileName());
    }

    @Test
    void testFileErrorEvent_ImplementsFileEvent() {
        var event = FileErrorEvent.builder()
                .fileName("error.txt")
                .originalFilePath("/mnt/new/error.txt")
                .sourceDirectory("/mnt")
                .timestamp(Instant.now())
                .errorMessage("File could not be read")
                .errorType("READ_ERROR")
                .build();

        assertInstanceOf(FileEvent.class, event);
        assertEquals("error.txt", event.getFileName());
    }

    @Test
    void testPatternMatching_FileProcessedEvent() {
        var event = FileProcessedEvent.builder()
                .fileName("test.txt")
                .originalFilePath("/mnt/new/test.txt")
                .processedFilePath("/mnt/processed/test-2025.txt")
                .sourceDirectory("/mnt")
                .timestamp(Instant.now())
                .build();

        var description = eventHandler.getEventDescription(event);
        assertTrue(description.contains("Successfully processed"));
        assertTrue(description.contains("test.txt"));
    }

    @Test
    void testPatternMatching_FileErrorEvent() {
        var event = FileErrorEvent.builder()
                .fileName("error.txt")
                .originalFilePath("/mnt/new/error.txt")
                .sourceDirectory("/mnt")
                .timestamp(Instant.now())
                .errorMessage("File could not be read")
                .errorType("READ_ERROR")
                .build();

        var description = eventHandler.getEventDescription(event);
        assertTrue(description.contains("Failed to process"));
        assertTrue(description.contains("error.txt"));
        assertTrue(description.contains("READ_ERROR"));
    }

    @Test
    void testSealedInterface_ExhaustivePatternMatching() {
        // This test verifies that the switch in FileEventHandler covers all cases
        // If we add a new type to the sealed interface, the compiler will require
        // us to update the switch statement
        
        var processedEvent = FileProcessedEvent.builder()
                .fileName("test.txt")
                .originalFilePath("/mnt/new/test.txt")
                .processedFilePath("/mnt/processed/test-2025.txt")
                .sourceDirectory("/mnt")
                .timestamp(Instant.now())
                .build();

        var errorEvent = FileErrorEvent.builder()
                .fileName("error.txt")
                .originalFilePath("/mnt/new/error.txt")
                .sourceDirectory("/mnt")
                .timestamp(Instant.now())
                .errorMessage("Test error")
                .errorType("TEST_ERROR")
                .build();

        // Both should produce descriptions without throwing exceptions
        assertNotNull(eventHandler.getEventDescription(processedEvent));
        assertNotNull(eventHandler.getEventDescription(errorEvent));
    }
}
