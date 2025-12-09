package com.example.common.outbox;

/**
 * Outbox 이벤트 발행 실패 시 발생하는 예외
 *
 * OutboxEventPublisher에서 이벤트를 데이터베이스에 저장하는 과정에서
 * 오류가 발생하면 이 예외를 던집니다.
 *
 * RuntimeException을 상속하여 명시적인 예외 처리를 강제하지 않으며,
 * 트랜잭션 롤백을 유발합니다.
 */
public class OutboxPublishException extends RuntimeException {
    public OutboxPublishException(String message) {
        super(message);
    }

    public OutboxPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
