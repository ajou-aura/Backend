package sulhoe.aura.service.login;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import sulhoe.aura.config.JwtTokenProvider;
import sulhoe.aura.handler.ApiException;

@Service
@RequiredArgsConstructor
public class SsoTokenExchangeService {

    private final SsoTicketService ssoTicketService;
    private final JwtTokenProvider jwtTokenProvider;
    private final AuthService authService;

    public ExchangeResult exchangeTicket(String code) {
        if (!StringUtils.hasText(code)) {
            throw new ApiException(
                    HttpStatus.BAD_REQUEST,
                    "요청 형식이 올바르지 않습니다.",
                    "MISSING_SSO_TICKET",
                    "code"
            );
        }

        SsoTicketService.Payload payload = ssoTicketService.consume(code);
        if (payload == null) {
            throw new ApiException(
                    HttpStatus.UNAUTHORIZED,
                    "유효하지 않거나 만료된 SSO 티켓입니다.",
                    "INVALID_SSO_TICKET",
                    "code"
            );
        }

        String accessToken = jwtTokenProvider.createAccessToken(payload.email(), payload.name());
        String refreshToken = authService.ssoRefresh(payload.email());
        return new ExchangeResult(
                accessToken,
                refreshToken,
                payload.signUp(),
                payload.email(),
                payload.name()
        );
    }

    public record ExchangeResult(
            String accessToken,
            String refreshToken,
            boolean signUp,
            String email,
            String name
    ) {
    }
}
