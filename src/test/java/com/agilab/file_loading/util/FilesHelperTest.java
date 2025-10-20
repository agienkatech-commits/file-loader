package com.agilab.file_loading.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FilesHelperTest {

    private final FilesHelper filesHelper = new FilesHelper(null);

    @Test
    void testGetNameWithoutExtension_WithExtension() {
        var result = filesHelper.getNameWithoutExtension("document.pdf");
        assertEquals("document", result);
    }

    @Test
    void testGetNameWithoutExtension_WithMultipleDots() {
        var result = filesHelper.getNameWithoutExtension("archive.tar.gz");
        assertEquals("archive.tar", result);
    }

    @Test
    void testGetNameWithoutExtension_WithoutExtension() {
        var result = filesHelper.getNameWithoutExtension("README");
        assertEquals("README", result);
    }

    @Test
    void testGetNameWithoutExtension_DotAtStart() {
        var result = filesHelper.getNameWithoutExtension(".gitignore");
        assertEquals(".gitignore", result);
    }

    @Test
    void testGetFileExtension_WithExtension() {
        var result = filesHelper.getFileExtension("document.pdf");
        assertEquals(".pdf", result);
    }

    @Test
    void testGetFileExtension_WithoutExtension() {
        var result = filesHelper.getFileExtension("README");
        assertEquals("", result);
    }

    @Test
    void testGetFileExtension_WithMultipleDots() {
        var result = filesHelper.getFileExtension("archive.tar.gz");
        assertEquals(".gz", result);
    }

    @Test
    void testGetFileExtension_DotAtStart() {
        var result = filesHelper.getFileExtension(".gitignore");
        assertEquals("", result);
    }
}
