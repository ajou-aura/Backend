package sulhoe.aura.dto.login;

public record AuthRefreshResponseDto(
        String accessToken,
        String refreshToken
) {
}
