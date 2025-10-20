package com.agilab.file_loading.exception;

/**
 * Exception thrown when a file is not found.
 */
public final class FileNotFoundException extends RuntimeException implements FileProcessingException {
    private final String filePath;

    public FileNotFoundException(String filePath, String message, Throwable cause) {
        super(message, cause);
        this.filePath = filePath;
    }

    public FileNotFoundException(String filePath, String message) {
        super(message);
        this.filePath = filePath;
    }

    @Override
    public String getFilePath() {
        return filePath;
    }
}
