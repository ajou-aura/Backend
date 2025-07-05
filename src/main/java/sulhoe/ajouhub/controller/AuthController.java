package sulhoe.ajouhub.controller;

import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.UriComponentsBuilder;
import sulhoe.ajouhub.dto.ApiResponse;
import sulhoe.ajouhub.dto.login.LoginResponseDto;
import sulhoe.ajouhub.service.login.AuthService;

import java.io.IOException;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;

    @Value("${oauth.google.client-id}")
    private String clientId;

    @Value("${oauth.google.redirect-uri}")
    private String redirectUri;

    // 프론트엔드 리다이렉트 주소 (예: http://localhost:3000)
    @Value("${app.frontend-url}")
    private String frontendUrl;

    // 구글 로그인 시작
    @GetMapping("/google")
    public void redirectToGoogle(HttpServletResponse res) throws IOException {
        String url = UriComponentsBuilder.fromUriString("https://accounts.google.com/o/oauth2/v2/auth")
                .queryParam("client_id", clientId)
                .queryParam("redirect_uri", redirectUri)
                .queryParam("response_type", "code")
                .queryParam("scope", "openid email profile")
                .build().toUriString();
        log.debug("[CTRL] Redirecting to Google OAuth URL: {}", url);

        res.sendRedirect(url);
    }

    // 콜백 처리 후 프론트로 리디렉트
    @GetMapping("/callback")
    @ResponseStatus(HttpStatus.SEE_OTHER)
    public void callback(@RequestParam String code, HttpServletResponse res) throws IOException {
        // 구글에서 받은 코드를 통해 토큰 발급
        log.debug("[CTRL] OAuth callback received, code={}", code);

        LoginResponseDto dto = authService.loginWithGoogle(code);
        // 프론트엔드 URL에 토큰을 쿼리로 붙여 리다이렉트
        String target = UriComponentsBuilder.fromUriString(frontendUrl)
                .queryParam("accessToken", dto.accessToken())
                .queryParam("refreshToken", dto.refreshToken())
                .build().toUriString();

        log.debug("[CTRL] Redirecting back to frontend with tokens: {}", target);
        res.sendRedirect(target);
    }

    // 리프레시 토큰으로 액세스 토큰만 재발급
    @PostMapping("/refresh")
    public ResponseEntity<ApiResponse<Map<String, String>>> refresh(@RequestBody Map<String, String> body) {
        log.debug("[CTRL] Refresh endpoint called with body={}", body);

        try {
            String refreshToken = body.get("refreshToken");
            if (refreshToken == null) {
                return ResponseEntity.badRequest().body(
                        ApiResponse.error(400, "리프레시 토큰이 누락되었습니다.", Map.of("code", "MISSING_REFRESH_TOKEN"))
                );
            }
            String access = authService.refreshAccessToken(refreshToken);
            return ResponseEntity.ok(ApiResponse.success(Map.of("accessToken", access)));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(
                    ApiResponse.error(400, "유효하지 않은 토큰입니다.", Map.of("code", "INVALID_REFRESH_TOKEN"))
            );
        }
    }
}
