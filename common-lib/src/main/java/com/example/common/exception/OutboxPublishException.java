package com.example.common.exception;

public class OutboxPublishException extends RuntimeException {
    public OutboxPublishException(String message) {
        super(message);
    }

    public OutboxPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
