package com.agilab.file_loading.exception;

/**
 * Exception thrown when a file cannot be read.
 */
public final class FileReadException extends RuntimeException implements FileProcessingException {
    private final String filePath;

    public FileReadException(String filePath, String message, Throwable cause) {
        super(message, cause);
        this.filePath = filePath;
    }

    public FileReadException(String filePath, String message) {
        super(message);
        this.filePath = filePath;
    }

    @Override
    public String getFilePath() {
        return filePath;
    }
}
