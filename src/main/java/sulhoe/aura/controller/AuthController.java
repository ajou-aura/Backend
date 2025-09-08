package sulhoe.aura.controller;

import jakarta.servlet.http.HttpServletRequest;
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
import org.springframework.security.web.csrf.CsrfToken;

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


    @GetMapping("/csrf")
    public ResponseEntity<Void> csrf(CsrfToken token) {
        // 쿠키(XSRF-TOKEN)는 repo가 내려주고,
        // 헤더로 값도 같이 내려 프런트가 읽어 저장하게 함
        return ResponseEntity.noContent()
                .header("X-CSRF-TOKEN", token.getToken())
                .build();
    }

    private ResponseCookie refreshCookie(String refresh) {
        return ResponseCookie.from("refreshToken", refresh)
                .httpOnly(true)
                .secure(true)
                .sameSite("None")
                .path("/")
                .maxAge(JwtTokenProvider.REFRESH_EXPIRY_SECONDS)
                .build();
    }

    private ResponseCookie webSessionCookie(String accessJwt) {
        return ResponseCookie.from("WEB_SESSION", accessJwt)
                .httpOnly(true)
                .secure(true)
                .sameSite("None")
                .path("/")
                .maxAge(JwtTokenProvider.WEB_ACCESS_EXP)
                .build();
    }

    // 추가: 삭제용 쿠키 빌더
    private ResponseCookie clearCookie(String name) {
        return ResponseCookie.from(name, "")
                .httpOnly(true)
                .secure(true)
                .sameSite("None")
                .path("/")
                .maxAge(0)
                .build();
    }

    // 구글 로그인 시작: ?mode=app | web
    @GetMapping("/google")
    public void redirectToGoogle(@RequestParam(defaultValue = "web") String mode,
                                 HttpServletResponse res) throws IOException {
        String url = UriComponentsBuilder.fromUriString("https://accounts.google.com/o/oauth2/v2/auth")
                .queryParam("client_id", clientId)
                .queryParam("redirect_uri", redirectUri)
                .queryParam("response_type", "code")
                .queryParam("scope", "openid email profile")
                .queryParam("state", mode) // 앱/웹 분기 신호
                .build().toUriString();
        log.debug("[CTRL] Redirecting to Google OAuth URL: {}", url);
        res.sendRedirect(url);
    }

    // 콜백 처리 후 프론트로 리디렉트
    // 쿠키 두 개(REFRESH + WEB_SESSION)만 심고 프론트로 이동
    @GetMapping("/callback")
    @ResponseStatus(HttpStatus.SEE_OTHER)
    public void callback(@RequestParam String code,
                         @RequestParam(required = false, defaultValue = "web") String state,
                         HttpServletResponse res) throws IOException {
        // 구글에서 받은 코드를 통해 토큰 발급
        log.info("[CTRL] OAuth callback received, code={}, state={}", code, state);

        LoginResponseDto dto = authService.loginWithGoogle(code);

        if ("app".equalsIgnoreCase(state)) {
            // 앱 모드: 딥링크로 토큰 전달
            String target = UriComponentsBuilder.fromUriString("aura://oauth-callback")
                    .queryParam("accessToken", dto.accessToken())
                    .queryParam("refreshToken", dto.refreshToken())
                    .queryParam("signUp", dto.signUp())
                    .build().toUriString();

            res.setStatus(HttpStatus.SEE_OTHER.value());
            res.setHeader(HttpHeaders.LOCATION, target);
            return;
        }

        // 웹 모드: 쿠키 세팅 후 프론트로 리다이렉트
        // refresh는 Http
        res.addHeader(HttpHeaders.SET_COOKIE, refreshCookie(dto.refreshToken()).toString());

        // 2) Access(JWT)를 WEB_SESSION 쿠키로 심음 (프론트는 쿠키 기반으로만 동작)
        res.addHeader(HttpHeaders.SET_COOKIE, webSessionCookie(dto.accessToken()).toString());

        String target = UriComponentsBuilder.fromUriString(frontendUrl)
                .queryParam("signUp", dto.signUp())
                .build().toUriString();

        res.setStatus(HttpStatus.SEE_OTHER.value());
        res.setHeader(HttpHeaders.LOCATION, target);
        log.info("[CTRL] Redirecting back to frontend with tokens: {}", target);
    }

    // 리프레시
    @PostMapping("/refresh")
    public ResponseEntity<ApiResponse<Map<String, String>>> refresh(
            @CookieValue(value = "refreshToken", required = false) String cookieRt,
            @RequestBody(required = false) Map<String, String> body,
            HttpServletResponse response
    ) {
        String bodyRt = (body != null ? body.get("refreshToken") : null);
        String rt = (bodyRt != null ? bodyRt : cookieRt);
        log.info("[REFRESH] called: source={}, hasRt={}",
                (bodyRt!=null ? "body" : (cookieRt!=null ? "cookie" : "none")),
                (rt!=null));

        if (rt == null) {
            return ResponseEntity.badRequest()
                    .body(ApiResponse.error(400, "리프레시 토큰이 누락되었습니다.", Map.of("code", "MISSING_REFRESH_TOKEN")));
        }

        try{
            var dto = authService.refreshAccessToken(rt); // (회전된 RT 포함)

            // 웹 호환: 쿠키도 항상 갱신해줌(앱은 무시)
            response.addHeader(HttpHeaders.SET_COOKIE, refreshCookie(dto.refreshToken()).toString());
            response.addHeader(HttpHeaders.SET_COOKIE, webSessionCookie(dto.accessToken()).toString());

            // 앱 호환: JSON도 내려줌(웹은 안 써도 됨)
            return ResponseEntity.ok(ApiResponse.success(
                    Map.of("accessToken", dto.accessToken(), "refreshToken", dto.refreshToken())
            ));
        } catch (RuntimeException e) {
            log.warn("[REFRESH] invalid/expired RT: {}", e.getMessage());

            response.addHeader(HttpHeaders.SET_COOKIE, clearCookie("refreshToken").toString());
            response.addHeader(HttpHeaders.SET_COOKIE, clearCookie("WEB_SESSION").toString());

            // 표준 오류 헤더 추가 -> 프런트가 401 원인을 쉽게 구분
            return ResponseEntity.status(401)
                    .header(HttpHeaders.WWW_AUTHENTICATE,
                            "Bearer error=\"invalid_token\", error_description=\"refresh_expired_or_invalid\"")
                    .body(ApiResponse.error(401, "Invalid refresh token", Map.of("code","INVALID_REFRESH_TOKEN")));
        }
    }


    // 앱 -> 웹 SSO 브리지 (Bearer Access를 WEB_SESSION 쿠키로 주입)
    @GetMapping("/sso/webview")
    public ResponseEntity<Void> ssoWebView(
            @RequestHeader(name = "Authorization", required = false) String authz) {

        String token = (authz != null && authz.startsWith("Bearer ")) ? authz.substring(7) : null;
        if (token == null || !jwt.validateToken(token)) {
            return ResponseEntity.status(401).build();
        }

        // 유효한 Access JWT를 그대로 WEB_SESSION으로 내려 프론트가 쿠키 기반 사용
        return ResponseEntity.status(302)
                .header(HttpHeaders.SET_COOKIE, webSessionCookie(token).toString())
                .header(HttpHeaders.LOCATION, frontendUrl + "?embed=app")
                .build();
    }
}
