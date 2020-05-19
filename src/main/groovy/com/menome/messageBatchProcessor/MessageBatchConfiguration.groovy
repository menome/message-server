package com.menome.messageBatchProcessor

class MessageBatchConfiguration {
    boolean createRelationshipNodes = false

    MessageBatchConfiguration(boolean createRelationshipNodes) {
        this.createRelationshipNodes = createRelationshipNodes
    }
}
