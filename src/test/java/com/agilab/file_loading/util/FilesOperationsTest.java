package com.agilab.file_loading.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class FilesOperationsTest {

    @TempDir
    Path tempDir;

    private Path testDir;

    @BeforeEach
    void setUp() throws IOException {
        testDir = tempDir.resolve("test");
        Files.createDirectories(testDir);
    }

    @Test
    void findNewFiles_shouldReturnRegularFiles() throws IOException, InterruptedException {
        // Given
        Path file1 = Files.createFile(testDir.resolve("file1.txt"));
        Path file2 = Files.createFile(testDir.resolve("file2.txt"));
        Files.write(file1, "content".getBytes());
        Files.write(file2, "content".getBytes());
        
        // Wait for files to be stable
        Thread.sleep(1100);

        // When
        List<Path> files = FilesOperations.findNewFiles(testDir);

        // Then
        assertThat(files).hasSize(2);
        assertThat(files).contains(file1, file2);
    }

    @Test
    void findNewFiles_shouldExcludeTemporaryFiles() throws IOException, InterruptedException {
        // Given
        Path normalFile = Files.createFile(testDir.resolve("normal.txt"));
        Path dotFile = Files.createFile(testDir.resolve(".hidden"));
        Path tildeFile = Files.createFile(testDir.resolve("~backup"));
        Path tmpFile = Files.createFile(testDir.resolve("temp.tmp"));
        
        Files.write(normalFile, "content".getBytes());
        Files.write(dotFile, "content".getBytes());
        Files.write(tildeFile, "content".getBytes());
        Files.write(tmpFile, "content".getBytes());
        
        // Wait for files to be stable
        Thread.sleep(1100);

        // When
        List<Path> files = FilesOperations.findNewFiles(testDir);

        // Then
        assertThat(files).hasSize(1);
        assertThat(files).contains(normalFile);
    }

    @Test
    void findNewFiles_shouldExcludeEmptyFiles() throws IOException, InterruptedException {
        // Given
        Path emptyFile = Files.createFile(testDir.resolve("empty.txt"));
        Path nonEmptyFile = Files.createFile(testDir.resolve("nonempty.txt"));
        Files.write(nonEmptyFile, "content".getBytes());
        
        // Wait for files to be stable
        Thread.sleep(1100);

        // When
        List<Path> files = FilesOperations.findNewFiles(testDir);

        // Then
        assertThat(files).hasSize(1);
        assertThat(files).contains(nonEmptyFile);
    }

    @Test
    void moveFileAtomically_shouldMoveFile() throws IOException {
        // Given
        Path sourceFile = Files.createFile(testDir.resolve("source.txt"));
        Files.write(sourceFile, "test content".getBytes());
        Path targetFile = testDir.resolve("target.txt");

        // When
        Path result = FilesOperations.moveFileAtomically(sourceFile, targetFile);

        // Then
        assertThat(result).isEqualTo(targetFile);
        assertThat(Files.exists(targetFile)).isTrue();
        assertThat(Files.exists(sourceFile)).isFalse();
        assertThat(Files.readString(targetFile)).isEqualTo("test content");
    }

    @Test
    void getNameWithoutExtension_shouldHandleRegularFiles() {
        // When/Then
        assertEquals("file", FilesOperations.getNameWithoutExtension("file.txt"));
        assertEquals("archive.tar", FilesOperations.getNameWithoutExtension("archive.tar.gz"));
        assertEquals("noextension", FilesOperations.getNameWithoutExtension("noextension"));
        assertEquals(".dotfile", FilesOperations.getNameWithoutExtension(".dotfile"));
    }

    @Test
    void getFileExtension_shouldHandleRegularFiles() {
        // When/Then
        assertEquals(".txt", FilesOperations.getFileExtension("file.txt"));
        assertEquals(".gz", FilesOperations.getFileExtension("archive.tar.gz"));
        assertEquals("", FilesOperations.getFileExtension("noextension"));
        assertEquals("", FilesOperations.getFileExtension(".dotfile"));
    }
}
