package com.example.common.exception;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

/**
 * API 에러 응답 표준 포맷
 *
 * 모든 마이크로서비스의 에러 응답을 일관된 형식으로 제공하여
 * 클라이언트가 에러를 예측 가능하게 처리할 수 있도록 합니다.
 *
 * <h3>응답 예시:</h3>
 * <pre>
 * {
 *   "timestamp": "2024-01-01T00:00:00Z",
 *   "status": 404,
 *   "error": "Not Found",
 *   "message": "Order not found: 12345",
 *   "validationErrors": null
 * }
 * </pre>
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ErrorResponse {
    /** 에러 발생 시각 (UTC 기준) */
    private Instant timestamp;

    /** HTTP 상태 코드 (예: 400, 404, 500) */
    private int status;

    /** 에러 유형 (예: "Bad Request", "Not Found") */
    private String error;

    /** 사용자에게 표시할 에러 메시지 */
    private String message;

    /**
     * 유효성 검증 실패 시 필드별 에러 메시지
     * (예: {"email": "이메일 형식이 올바르지 않습니다"})
     * 유효성 검증 에러가 아닌 경우 null
     */
    private Map<String, String> validationErrors;
}
