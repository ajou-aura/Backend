package sulhoe.ajouhub.controller;

import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseCookie;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.UriComponentsBuilder;
import sulhoe.ajouhub.config.JwtTokenProvider;
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

    // 프론트엔드 리다이렉트 주소 (: http://localhost:3000)
    @Value("${app.frontend-url}")
    private String frontendUrl;

    // 구글 로그인 시작
    @GetMapping("/google")
    public void redirectToGoogle(HttpServletResponse res) throws IOException {
        String url = UriComponentsBuilder.fromUriString("https://accounts.google.com/o/oauth2/v2/auth").queryParam("client_id", clientId).queryParam("redirect_uri", redirectUri).queryParam("response_type", "code").queryParam("scope", "openid email profile").build().toUriString();
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

        boolean local  = request.getServerName().equals("localhost") || request.getServerName().startsWith("127.");

        // refresh token은 쿠키로
        ResponseCookie cookie = ResponseCookie.from("refreshToken", dto.refreshToken())
                .httpOnly(true)
                .secure(!local)                    // 로컬: false, 운영: true
                .sameSite(local ? "Lax" : "None")  // Lax는 Secure 필요 없음
                .path("/")
                .maxAge(JwtTokenProvider.REFRESH_EXPIRY_SECONDS)
                .build();

        log.debug("🍪 Set-Cookie → {}", cookie);   // 실제 속성 확인
        res.setHeader(HttpHeaders.SET_COOKIE, cookie.toString());

        // 프론트엔드 URL에 토큰을 쿼리로 붙여 리다이렉트
        String target = UriComponentsBuilder
                .fromUriString(frontendUrl)
                .queryParam("accessToken", dto.accessToken())
                .queryParam("signUp", dto.signUp())
                .build().toUriString();

        log.debug("[CTRL] Redirecting back to frontend with tokens: {}", target);
        res.sendRedirect(target);
    }

    // 리프레시 토큰으로 액세스 토큰만 재발급
    @PostMapping("/refresh")
    public ResponseEntity<ApiResponse<Map<String, String>>> refresh(@CookieValue(value = "refreshToken", required = false) String refreshToken, HttpServletResponse response) {
        if (refreshToken == null) {
            return ResponseEntity.badRequest().body(ApiResponse.error(400, "리프레시 토큰이 누락되었습니다.", Map.of("code", "MISSING_REFRESH_TOKEN")));
        }
        try {
            var dto = authService.refreshAccessToken(refreshToken);
            // 새로운 리프레시 토큰 쿠키로 설정
            ResponseCookie cookie = ResponseCookie.from("refreshToken", dto.refreshToken())
                    .httpOnly(true)
                    .secure(true)
                    .sameSite("None")
                    .path("/")
                    .maxAge(JwtTokenProvider.REFRESH_EXPIRY_SECONDS)
                    .build();
            response.setHeader(HttpHeaders.SET_COOKIE, cookie.toString());

            return ResponseEntity.ok(ApiResponse.success(Map.of("accessToken", dto.accessToken())));
        } catch (RuntimeException e) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(ApiResponse.error(401, e.getMessage(), Map.of("code", "INVALID_REFRESH_TOKEN")));
        }
    }
}
