package com.menome.messageBatchProcessor

import groovy.transform.Canonical

import java.math.RoundingMode
import java.time.Duration

@Canonical
class MessageBatchSummary {
    int successCount
    int errorCount
    Duration batchProcessingDuration
    BigDecimal rate

    MessageBatchSummary(int successCount, int errorCount, Duration batchProcessingDuration) {
        this.successCount = successCount
        this.errorCount = errorCount
        this.batchProcessingDuration = batchProcessingDuration
        BigDecimal millis = new BigDecimal(this.getBatchProcessingDuration().toMillis());
        if (millis == BigDecimal.ZERO) {
            millis = BigDecimal.ONE
        }
        BigDecimal rate = new BigDecimal(this.successCount).divide(millis, 3, RoundingMode.HALF_EVEN).multiply BigDecimal.valueOf(1000)
        this.rate = rate.setScale(0, RoundingMode.HALF_EVEN)
    }


    @Override
    String toString() {

        return "MessageBatchSummary{" +
                "successCount=" + successCount +
                ", errorCount=" + errorCount +
                ", duration (s)=" + batchProcessingDuration.toSeconds() +
                ", duration (ms)=" + batchProcessingDuration.toMillis() +
                ", rate (per s)=" + rate +
                '}';
    }
}
