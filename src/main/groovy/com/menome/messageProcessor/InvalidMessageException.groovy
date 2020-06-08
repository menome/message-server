package com.menome.messageProcessor

class InvalidMessageException extends RuntimeException {
    InvalidMessageException(String message) {
        super(message)
    }
}
