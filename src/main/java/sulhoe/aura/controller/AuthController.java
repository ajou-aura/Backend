package sulhoe.aura.controller;

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
import sulhoe.aura.config.JwtTokenProvider;
import sulhoe.aura.dto.ApiResponse;
import sulhoe.aura.dto.login.LoginResponseDto;
import sulhoe.aura.service.login.AuthService;

import java.io.IOException;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;
    private final JwtTokenProvider jwt;

    @Value("${oauth.google.client-id}")
    private String clientId;

    @Value("${oauth.google.redirect-uri}")
    private String redirectUri;

    // 프론트엔드 리다이렉트 주소 (: http://localhost:3000)
    @Value("${app.frontend-url}")
    private String frontendUrl;

    @Value("${app.is-prod:false}") // 배포 환경
    private boolean isProd;

    private ResponseCookie refreshCookie(String refresh) {
        return ResponseCookie.from("refreshToken", refresh)
                .httpOnly(true)
                .secure(true)
                .sameSite(isProd ? "Lax" : "None")
                .path("/")
                .maxAge(JwtTokenProvider.REFRESH_EXPIRY_SECONDS)
                .build();
    }

    private ResponseCookie webSessionCookie(String accessJwt) {
        return ResponseCookie.from("WEB_SESSION", accessJwt)
                .httpOnly(true)
                .secure(true)
                .sameSite(isProd ? "Lax" : "None")
                .path("/")
                // .domain(".ajouhub.kr") // 프론트/백이 서브도메인 분리면 환경에 맞게 설정
                // maxAge는 생략(세션 쿠키)하거나 Access 만료에 맞춰 설정해도 됨
                .build();
    }

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
    // 쿠키 두 개(REFRESH + WEB_SESSION)만 심고 프론트로 이동
    @GetMapping("/callback")
    @ResponseStatus(HttpStatus.SEE_OTHER)
    public void callback(@RequestParam String code, HttpServletResponse res) throws IOException {
        // 구글에서 받은 코드를 통해 토큰 발급
        log.debug("[CTRL] OAuth callback received, code={}", code);

        LoginResponseDto dto = authService.loginWithGoogle(code);

        // 1) Refresh는 그대로 HttpOnly 쿠키
        res.addHeader(HttpHeaders.SET_COOKIE, refreshCookie(dto.refreshToken()).toString());

        // 2) Access(JWT)를 WEB_SESSION 쿠키로 심음 (프론트는 쿠키 기반으로만 동작)
        res.addHeader(HttpHeaders.SET_COOKIE, webSessionCookie(dto.accessToken()).toString());

        String target = UriComponentsBuilder
                .fromUriString(frontendUrl)
                .queryParam("signUp", dto.signUp()) // 필요시 비민감 값만
                .build().toUriString();

        res.setStatus(HttpStatus.SEE_OTHER.value());
        res.setHeader(HttpHeaders.LOCATION, target);

        log.debug("[CTRL] Redirecting back to frontend with tokens: {}", target);
    }

    // 리프레시 토큰으로 액세스 토큰만 재발급
    @PostMapping("/refresh")
    public ResponseEntity<ApiResponse<Map<String, String>>> refresh(
            @CookieValue(value = "refreshToken", required = false) String refreshToken,
            @RequestParam(value = "web", required = false, defaultValue = "false") boolean web,
            HttpServletResponse response) {
        if (refreshToken == null) {
            return ResponseEntity.badRequest()
                    .body(ApiResponse.error(400, "리프레시 토큰이 누락되었습니다.", Map.of("code", "MISSING_REFRESH_TOKEN")));
        }
        try {
            var dto = authService.refreshAccessToken(refreshToken);
            // 리프레시 토큰 회전
            response.setHeader(HttpHeaders.SET_COOKIE, refreshCookie(dto.refreshToken()).toString());
            if (web) {
                // ★ 웹 모드: JSON 대신 WEB_SESSION 쿠키 갱신만 하고 204
                response.addHeader(HttpHeaders.SET_COOKIE, webSessionCookie(dto.accessToken()).toString());
                return ResponseEntity.noContent().build();
            } else {
                // 앱 모드: JSON으로 accessToken 반환(기존 호환)
                return ResponseEntity.ok(ApiResponse.success(Map.of("accessToken", dto.accessToken())));
            }

        } catch (RuntimeException e) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(ApiResponse.error(401, e.getMessage(), Map.of("code", "INVALID_REFRESH_TOKEN")));
        }
    }

    // 앱 -> 웹 SSO 브리지 (앱이 가진 Bearer Access로 웹뷰 쿠키 주입)
    @GetMapping("/sso/webview")
    public ResponseEntity<Void> ssoWebView(
            @RequestHeader(name = "Authorization", required = false) String authz) {

        String token = (authz != null && authz.startsWith("Bearer ")) ? authz.substring(7) : null;
        if (token == null || !jwt.validateToken(token)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

        // 유효한 Access JWT를 그대로 WEB_SESSION으로 내려 프론트가 쿠키 기반 사용
        return ResponseEntity.status(302)
                .header(HttpHeaders.SET_COOKIE, webSessionCookie(token).toString())
                .header(HttpHeaders.LOCATION, frontendUrl + "?embed=app")
                .build();
    }
}
