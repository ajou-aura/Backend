package sulhoe.aura.handler;

import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public class ApiException extends RuntimeException {
    private final HttpStatus status;   // 409 등
    private final String errorCode;    // CONFLICT_WITH_GLOBAL, DUPLICATE_PERSONAL, VALIDATION_ERROR ...
    private final String field;        // "phrase" 등

    public ApiException(HttpStatus status, String message, String errorCode, String field) {
        super(message);
        this.status = status;
        this.errorCode = errorCode;
        this.field = field;
    }
}
