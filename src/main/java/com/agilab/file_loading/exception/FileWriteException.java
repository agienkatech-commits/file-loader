package com.agilab.file_loading.exception;

/**
 * Exception thrown when a file cannot be written.
 */
public final class FileWriteException extends RuntimeException implements FileProcessingException {
    private final String filePath;

    public FileWriteException(String filePath, String message, Throwable cause) {
        super(message, cause);
        this.filePath = filePath;
    }

    public FileWriteException(String filePath, String message) {
        super(message);
        this.filePath = filePath;
    }

    @Override
    public String getFilePath() {
        return filePath;
    }
}
