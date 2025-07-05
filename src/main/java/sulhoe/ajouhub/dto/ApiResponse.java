package sulhoe.ajouhub.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ApiResponse<T> {
    private String status;
    private int code;
    private String message;
    private T data;

    public static <T> ApiResponse<T> success(T data) {
        return new ApiResponse<>("success", 200, "요청이 성공적으로 처리되었습니다.", data);
    }

    public static <T> ApiResponse<T> error(int code, String message, T errorData) {
        return new ApiResponse<>("error", code, message, errorData);
    }
}