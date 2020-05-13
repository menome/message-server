package com.menome.messageBatchProcessor

import groovy.transform.Canonical

import java.time.Duration

@Canonical
class MessageBatchSummary {
    int successCount
    int errorCount

    Duration batchProcessingDuration

    MessageBatchSummary(int successCount, int errorCount, Duration batchProcessingDuration) {
        this.successCount = successCount
        this.errorCount = errorCount
        this.batchProcessingDuration = batchProcessingDuration
    }


    @Override
    String toString() {
        return "MessageBatchSummary{" +
                "successCount=" + successCount +
                ", errorCount=" + errorCount +
                ", duration (s)=" + batchProcessingDuration.toSeconds() +
                ", duration (ms)=" + batchProcessingDuration.toMillis() +
                '}';
    }
}
