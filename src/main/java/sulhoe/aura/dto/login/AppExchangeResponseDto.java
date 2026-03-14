package sulhoe.aura.dto.login;

public record AppExchangeResponseDto(
        String accessToken,
        String refreshToken,
        boolean signUp,
        AuthUserDto user
) {
}
