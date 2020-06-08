package com.menome.messageBatchProcessor

import groovy.transform.Canonical

@Canonical
class MessageError {
    String errorText
    String message

    MessageError(String errorText, String message) {
        this.errorText = errorText
        this.message = message
    }


    @Override
    String toString() {
        return "MessageError{" +
                "errorText='" + errorText + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
