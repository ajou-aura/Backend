package sulhoe.aura.dto.login;

public record LoginResponseDto(
        String accessToken,
        String refreshToken,
        boolean signUp
) {}
