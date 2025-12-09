package com.example.common.exception;

import com.example.common.outbox.OutboxPublishException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * 전역 예외 처리 핸들러
 *
 * 모든 REST 컨트롤러에서 발생하는 예외를 중앙집중적으로 처리하여
 * 일관된 에러 응답 형식을 제공합니다.
 *
 * <h3>처리 방식:</h3>
 * 1. 비즈니스 예외: 적절한 HTTP 상태 코드와 메시지로 변환
 * 2. 유효성 검증 실패: 400 Bad Request + 필드별 에러 메시지
 * 3. 예상치 못한 예외: 500 Internal Server Error (민감한 정보 노출 방지)
 *
 * <h3>적용 범위:</h3>
 * @RestControllerAdvice 어노테이션으로 모든 @RestController에 자동 적용됩니다.
 */
@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    /**
     * 주문을 찾을 수 없을 때 발생 (404 Not Found)
     */
    @ExceptionHandler(OrderNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleOrderNotFoundException(OrderNotFoundException ex) {
        log.error("Order not found: {}", ex.getMessage());
        ErrorResponse error = ErrorResponse.builder()
                .timestamp(Instant.now())
                .status(HttpStatus.NOT_FOUND.value())
                .error("Not Found")
                .message(ex.getMessage())
                .build();
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(error);
    }

    /**
     * Outbox 이벤트 발행 실패 시 발생 (500 Internal Server Error)
     *
     * 이 예외는 트랜잭션 롤백을 유발하므로, 클라이언트에게는
     * 내부 에러로 응답하고 상세한 스택트레이스는 로그에만 기록합니다.
     */
    @ExceptionHandler(OutboxPublishException.class)
    public ResponseEntity<ErrorResponse> handleOutboxPublishException(OutboxPublishException ex) {
        log.error("Failed to publish outbox event: {}", ex.getMessage(), ex);
        ErrorResponse error = ErrorResponse.builder()
                .timestamp(Instant.now())
                .status(HttpStatus.INTERNAL_SERVER_ERROR.value())
                .error("Internal Server Error")
                .message("Failed to publish event")
                .build();
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
    }

    /**
     * 요청 본문 유효성 검증 실패 시 발생 (400 Bad Request)
     *
     * @Valid 어노테이션으로 검증한 DTO의 제약 조건 위반 시 발생합니다.
     * 필드별 에러 메시지를 포함하여 클라이언트가 정확히 어떤 필드가
     * 잘못되었는지 알 수 있도록 합니다.
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationException(MethodArgumentNotValidException ex) {
        log.error("Validation failed: {}", ex.getMessage());

        // 필드별 에러 메시지 수집
        Map<String, String> validationErrors = new HashMap<>();
        for (FieldError error : ex.getBindingResult().getFieldErrors()) {
            validationErrors.put(error.getField(), error.getDefaultMessage());
        }

        ErrorResponse error = ErrorResponse.builder()
                .timestamp(Instant.now())
                .status(HttpStatus.BAD_REQUEST.value())
                .error("Validation Failed")
                .message("Invalid request parameters")
                .validationErrors(validationErrors)
                .build();
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
    }

    /**
     * 잘못된 인자 전달 시 발생 (400 Bad Request)
     *
     * 비즈니스 로직 내에서 throw new IllegalArgumentException()으로
     * 잘못된 입력을 감지했을 때 처리합니다.
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgumentException(IllegalArgumentException ex) {
        log.error("Invalid argument: {}", ex.getMessage());
        ErrorResponse error = ErrorResponse.builder()
                .timestamp(Instant.now())
                .status(HttpStatus.BAD_REQUEST.value())
                .error("Bad Request")
                .message(ex.getMessage())
                .build();
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
    }

    /**
     * 처리되지 않은 모든 예외의 폴백 핸들러 (500 Internal Server Error)
     *
     * 위의 특정 예외 핸들러에서 처리하지 못한 모든 예외를 포괄합니다.
     * 보안을 위해 상세한 에러 메시지는 로그에만 기록하고,
     * 클라이언트에게는 일반적인 메시지만 반환합니다.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGenericException(Exception ex) {
        log.error("Unexpected error occurred: {}", ex.getMessage(), ex);
        ErrorResponse error = ErrorResponse.builder()
                .timestamp(Instant.now())
                .status(HttpStatus.INTERNAL_SERVER_ERROR.value())
                .error("Internal Server Error")
                .message("An unexpected error occurred")
                .build();
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
    }
}
