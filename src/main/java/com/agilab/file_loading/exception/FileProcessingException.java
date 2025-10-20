package com.agilab.file_loading.exception;

/**
 * Sealed hierarchy for file processing exceptions.
 * Demonstrates Java 25 sealed classes for better exception handling.
 */
public sealed interface FileProcessingException 
        permits FileReadException, FileWriteException, FileNotFoundException {
    
    String getFilePath();
    String getMessage();
    Throwable getCause();
}
