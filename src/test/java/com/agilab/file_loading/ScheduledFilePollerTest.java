package com.agilab.file_loading;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ScheduledFilePollerTest {

    @Mock
    private FileProcessingService fileProcessingService;

    @InjectMocks
    private ScheduledFilePoller scheduledFilePoller;

    @Test
    void pollBlobContainers_shouldCallProcessNewFiles() {
        // When
        scheduledFilePoller.pollBlobContainers();

        // Then
        verify(fileProcessingService, times(1)).processNewFiles();
    }

    @Test
    void pollBlobContainers_shouldHandleExceptions() {
        // Given
        doThrow(new RuntimeException("Test exception")).when(fileProcessingService).processNewFiles();

        // When - should not throw exception
        scheduledFilePoller.pollBlobContainers();

        // Then
        verify(fileProcessingService, times(1)).processNewFiles();
    }
}
