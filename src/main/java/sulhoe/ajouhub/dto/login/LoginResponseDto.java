package sulhoe.ajouhub.dto.login;

public record LoginResponseDto(
        String accessToken,
        String refreshToken,
        boolean signUp
) {}
