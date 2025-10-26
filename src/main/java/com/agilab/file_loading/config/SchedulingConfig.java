package com.agilab.file_loading.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
@ConditionalOnBooleanProperty(prefix = "file-loader", name = "scheduling.enabled", havingValue = true, matchIfMissing = true)
public class SchedulingConfig {
}
