package com.menome.messageBatchProcessor

import groovy.transform.Canonical

@Canonical
class MessageBatchResult{
    MessageBatchSummary batchSummary
    List<MessageError>errors

    MessageBatchResult(MessageBatchSummary batchSummary, List<MessageError> errors) {
        this.batchSummary = batchSummary
        this.errors = errors
    }
}
