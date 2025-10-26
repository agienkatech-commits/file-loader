package com.agilab.file_loading;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class FileLoadingApplication {

    public static void main(String[] args) {
        SpringApplication.run(FileLoadingApplication.class, args);
    }
}
