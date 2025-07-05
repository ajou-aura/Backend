package sulhoe.ajouhub.handler;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import sulhoe.ajouhub.dto.ApiResponse;

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
                                "message", ex.getMessage()
                        ))
                ))
        );
    }
}