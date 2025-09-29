package sulhoe.aura.handler;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;
import sulhoe.aura.dto.ApiResponse;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiResponse<Object>> handleValidation(MethodArgumentNotValidException ex) {
        List<Map<String, String>> errors = ex.getBindingResult().getFieldErrors().stream()
                .map(error -> Map.of(
                        "code", "VALIDATION_ERROR",
                        "field", error.getField(),
                        "message", Objects.requireNonNull(error.getDefaultMessage())
                )).toList();

        return ResponseEntity.badRequest().body(
                ApiResponse.error(400, "요청 형식이 올바르지 않습니다.", Map.of("errors", errors))
        );
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<Object>> handleGenericException(Exception ex) {
        log.error("Unhandled exception", ex);
        return ResponseEntity.internalServerError().body(
                ApiResponse.error(500, "서버 내부 오류가 발생했습니다.", Map.of(
                        "errors", List.of(Map.of(
                                "code", "INTERNAL_SERVER_ERROR",
                                "message", "예기치 않은 오류가 발생했습니다. 관리자에게 문의하세요."
                        ))
                ))
        );
    }

    @ExceptionHandler(sulhoe.aura.handler.ApiException.class)
    public ResponseEntity<ApiResponse<Object>> handleApiException(sulhoe.aura.handler.ApiException ex) {
        return ResponseEntity.status(ex.getStatus())
                .headers(ex.getHeaders())
                        .body(
                ApiResponse.error(
                        ex.getStatus().value(),
                        ex.getMessage(),
                        Map.of("errors", List.of(Map.of(
                                "code", ex.getErrorCode(),
                                "field", ex.getField(),
                                "message", ex.getMessage()
                        )))
                )
        );
    }

    @ExceptionHandler(org.springframework.web.server.ResponseStatusException.class)
    public ResponseEntity<ApiResponse<Object>> handleRSE(ResponseStatusException ex) {
        int http = ex.getStatusCode().value();
        String msg = ex.getReason() != null ? ex.getReason() : "요청 형식이 올바르지 않습니다.";
        return ResponseEntity.status(ex.getStatusCode()).body(
                ApiResponse.error(http, msg, Map.of(
                        "errors", List.of(Map.of(
                                "code", http == 400 ? "VALIDATION_ERROR" : "GENERIC_ERROR",
                                "message", msg
                        ))
                ))
        );
    }

    @ExceptionHandler(org.springframework.security.core.AuthenticationException.class)
    public ResponseEntity<ApiResponse<Object>> handleAuth(org.springframework.security.core.AuthenticationException ex) {
        return ResponseEntity.status(401).body(
                ApiResponse.error(401, "인증이 필요합니다.", Map.of(
                        "errors", List.of(Map.of("code","UNAUTHORIZED","message","로그인이 필요합니다."))
                ))
        );
    }

    @ExceptionHandler(org.springframework.security.access.AccessDeniedException.class)
    public ResponseEntity<ApiResponse<Object>> handleDenied(org.springframework.security.access.AccessDeniedException ex) {
        return ResponseEntity.status(403).body(
                ApiResponse.error(403, "접근이 거부되었습니다.", Map.of(
                        "errors", List.of(Map.of("code","FORBIDDEN","message","권한이 없습니다."))
                ))
        );
    }
}