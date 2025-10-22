package com.agilab.clients;

import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class ElasticsearchMessageVerifier {

    public boolean isMessageDelivered(String messageId, Duration maxAge) {
        try {
            var query = QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("messageId", messageId))
                    .must(QueryBuilders.rangeQuery("@timestamp")
                            .gte(Instant.now().minus(maxAge)));

            var response = elasticsearchClient.search(SearchRequest.of(s -> s
                    .index("kafka-messages-*")
                    .query(query)
                    .size(1)), Object.class);

            return response.hits().total().value() > 0;

        } catch (Exception e) {
            log.error("Failed to verify message delivery for ID: {}", messageId, e);
            return false;
        }
    }

    // Alternative: Check by file metadata
    public boolean isFileMessageDelivered(Path file, String baseDirectory) {
        var fileName = file.getFileName().toString();
        var query = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("fileName", fileName))
                .must(QueryBuilders.termQuery("baseDirectory", baseDirectory))
                .must(QueryBuilders.rangeQuery("@timestamp")
                        .gte(getFileCreationTime(file)));

        // ... search logic
    }
}
