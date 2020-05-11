package com.menome.errorHandler

class ErrorHandlerHelper {
    static Tuple2<String, String> toTupple(Exception ex, String message) {
        return new Tuple2<>(ex.toString(), message)

    }
}
