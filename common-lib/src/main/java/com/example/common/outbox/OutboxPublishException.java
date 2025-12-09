package com.example.common.outbox;

public class OutboxPublishException extends RuntimeException {
    public OutboxPublishException(String message) {
        super(message);
    }

    public OutboxPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
