package com.example.common.exception;

/**
 * 주문을 찾을 수 없을 때 발생하는 예외
 *
 * 주문 ID로 조회했으나 해당 주문이 존재하지 않을 때 발생합니다.
 * GlobalExceptionHandler에서 이 예외를 포착하여 404 Not Found 응답으로 변환합니다.
 *
 * RuntimeException을 상속하므로 명시적인 예외 처리가 필요하지 않으며,
 * 트랜잭션 롤백을 유발합니다.
 */
public class OrderNotFoundException extends RuntimeException {
    public OrderNotFoundException(String message) {
        super(message);
    }

    public OrderNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
