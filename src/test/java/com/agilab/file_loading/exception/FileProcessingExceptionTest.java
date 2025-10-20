package com.agilab.file_loading.exception;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for sealed FileProcessingException hierarchy.
 * Demonstrates Java 25 sealed classes with pattern matching.
 */
class FileProcessingExceptionTest {

    private final FileExceptionHandler exceptionHandler = new FileExceptionHandler();

    @Test
    void testFileReadException_ImplementsInterface() {
        var exception = new FileReadException("/test/file.txt", "Cannot read file");
        
        assertInstanceOf(FileProcessingException.class, exception);
        assertEquals("/test/file.txt", exception.getFilePath());
        assertEquals("Cannot read file", exception.getMessage());
    }

    @Test
    void testFileWriteException_ImplementsInterface() {
        var exception = new FileWriteException("/test/file.txt", "Cannot write file");
        
        assertInstanceOf(FileProcessingException.class, exception);
        assertEquals("/test/file.txt", exception.getFilePath());
        assertEquals("Cannot write file", exception.getMessage());
    }

    @Test
    void testFileNotFoundException_ImplementsInterface() {
        var exception = new FileNotFoundException("/test/file.txt", "File not found");
        
        assertInstanceOf(FileProcessingException.class, exception);
        assertEquals("/test/file.txt", exception.getFilePath());
        assertEquals("File not found", exception.getMessage());
    }

    @Test
    void testPatternMatching_ErrorType() {
        var readException = new FileReadException("/test/file.txt", "Read error");
        var writeException = new FileWriteException("/test/file.txt", "Write error");
        var notFoundException = new FileNotFoundException("/test/file.txt", "Not found");

        assertEquals("READ_ERROR", exceptionHandler.getErrorType(readException));
        assertEquals("WRITE_ERROR", exceptionHandler.getErrorType(writeException));
        assertEquals("NOT_FOUND", exceptionHandler.getErrorType(notFoundException));
    }

    @Test
    void testPatternMatching_ErrorSeverity() {
        var readException = new FileReadException("/test/file.txt", "Read error");
        var writeException = new FileWriteException("/test/file.txt", "Write error");
        var notFoundException = new FileNotFoundException("/test/file.txt", "Not found");

        assertEquals("MEDIUM", exceptionHandler.getErrorSeverity(readException));
        assertEquals("HIGH", exceptionHandler.getErrorSeverity(writeException));
        assertEquals("LOW", exceptionHandler.getErrorSeverity(notFoundException));
    }

    @Test
    void testExhaustivePatternMatching() {
        // This test verifies that the switch covers all sealed subtypes
        // The compiler ensures exhaustiveness with sealed types
        
        var exceptions = new FileProcessingException[] {
            new FileReadException("/test/read.txt", "Read error"),
            new FileWriteException("/test/write.txt", "Write error"),
            new FileNotFoundException("/test/missing.txt", "Not found")
        };

        for (var exception : exceptions) {
            // All should return a valid error type without throwing
            assertNotNull(exceptionHandler.getErrorType(exception));
            assertNotNull(exceptionHandler.getErrorSeverity(exception));
        }
    }

    @Test
    void testExceptionLogging_DoesNotThrow() {
        var readException = new FileReadException("/test/file.txt", "Read error");
        var writeException = new FileWriteException("/test/file.txt", "Write error");
        var notFoundException = new FileNotFoundException("/test/file.txt", "Not found");

        // Should not throw any exceptions
        assertDoesNotThrow(() -> exceptionHandler.logException(readException));
        assertDoesNotThrow(() -> exceptionHandler.logException(writeException));
        assertDoesNotThrow(() -> exceptionHandler.logException(notFoundException));
    }
}
