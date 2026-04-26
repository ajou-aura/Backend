package sulhoe.aura.controller;

import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.HtmlUtils;
import org.springframework.web.util.UriComponentsBuilder;
import sulhoe.aura.config.JwtTokenProvider;
import sulhoe.aura.config.OAuthProperties;
import sulhoe.aura.dto.ApiResponse;
import sulhoe.aura.dto.login.AppExchangeRequestDto;
import sulhoe.aura.dto.login.AppExchangeResponseDto;
import sulhoe.aura.dto.login.AuthRefreshResponseDto;
import sulhoe.aura.dto.login.AuthTokenRequestDto;
import sulhoe.aura.dto.login.AuthUserDto;
import sulhoe.aura.dto.login.LoginResponseDto;
import sulhoe.aura.handler.ApiException;
import sulhoe.aura.service.login.AuthService;
import sulhoe.aura.service.login.SsoTokenExchangeService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.springframework.security.web.csrf.CsrfToken;
import sulhoe.aura.service.login.SsoTicketService;

@Slf4j
@RestController
@RequestMapping("/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;
    private final JwtTokenProvider jwt;
    private final SsoTicketService ssoTicketService;
    private final SsoTokenExchangeService ssoTokenExchangeService;
    private final OAuthProperties oauthProperties;

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
                .maxAge(JwtTokenProvider.WEB_ACCESS_EXPIRY_SECONDS)
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

    private void writeSessionCookies(HttpServletResponse response, String accessToken, String refreshToken) {
        response.addHeader(HttpHeaders.SET_COOKIE, refreshCookie(refreshToken).toString());
        response.addHeader(HttpHeaders.SET_COOKIE, webSessionCookie(accessToken).toString());
    }

    private void clearAuthCookies(HttpServletResponse response) {
        response.addHeader(HttpHeaders.SET_COOKIE, clearCookie("refreshToken").toString());
        response.addHeader(HttpHeaders.SET_COOKIE, clearCookie("WEB_SESSION").toString());
    }

    private String resolveRefreshToken(String cookieRt, AuthTokenRequestDto body) {
        String bodyRt = body != null ? body.refreshToken() : null;
        if (StringUtils.hasText(bodyRt)) {
            return bodyRt;
        }
        return StringUtils.hasText(cookieRt) ? cookieRt : null;
    }

    private String tokenSource(String cookieRt, AuthTokenRequestDto body) {
        String bodyRt = body != null ? body.refreshToken() : null;
        if (StringUtils.hasText(bodyRt)) {
            return "body";
        }
        if (StringUtils.hasText(cookieRt)) {
            return "cookie";
        }
        return "none";
    }

    // 구글 로그인 시작: ?mode=web | android | ios | app (app is treated as android)
    @GetMapping("/google")
    public void redirectToGoogle(@RequestParam(defaultValue = "web") String mode,
                                 HttpServletResponse res) throws IOException {
        // Normalize mode: "app" is treated as "android" for backward compatibility
        String normalizedMode = "app".equalsIgnoreCase(mode) ? "android" : mode;

        OAuthProperties.ClientConfig config = oauthProperties.resolve(normalizedMode);

        String nonce = ssoTicketService.generateStateNonce(normalizedMode);
        String state = java.util.Base64.getUrlEncoder().withoutPadding()
                .encodeToString((nonce + ":" + normalizedMode).getBytes(StandardCharsets.UTF_8));

        String url = UriComponentsBuilder.fromUriString("https://accounts.google.com/o/oauth2/v2/auth")
                .queryParam("client_id", config.getClientId())
                .queryParam("redirect_uri", config.getRedirectUri())
                .queryParam("response_type", "code")
                .queryParam("scope", "openid email profile")
                .queryParam("state", state)
                .build().toUriString();
        log.debug("[CTRL] Redirecting to Google OAuth URL for platform '{}': redirectUri={}",
                normalizedMode, config.getRedirectUri());
        res.sendRedirect(url);
    }

    // 콜백 처리 후 프론트로 리디렉트
    // 쿠키 두 개(REFRESH + WEB_SESSION)만 심고 프론트로 이동
    @GetMapping("/callback")
    public void callback(@RequestParam(required = false) String code,
                         @RequestParam(required = false, defaultValue = "") String state,
                         @RequestParam(required = false, name = "error") String oauthError,
                         @RequestParam(required = false, name = "error_description") String oauthErrorDesc,
                         HttpServletResponse res) throws IOException {
        try {
            if (code == null) {
                throw new ApiException(
                        org.springframework.http.HttpStatus.BAD_REQUEST,
                        (oauthErrorDesc != null && !oauthErrorDesc.isBlank()) ? oauthErrorDesc : "유효하지 않은 인가 코드입니다.",
                        "OAUTH_CALLBACK_ERROR",
                        "code"
                );
            }

            String mode = resolveAndValidateState(state);
            LoginResponseDto dto = authService.loginWithGoogle(code, mode);

            if ("android".equals(mode) || "ios".equals(mode) || "app".equals(mode)) {
                String email = jwt.getEmail(dto.accessToken());
                String name  = jwt.getName(dto.accessToken());
                String ticket = ssoTicketService.issue(email, name, dto.signUp());
                String target = UriComponentsBuilder.fromUriString("aura://oauth-callback")
                        .queryParam("code", ticket)
                        .build(true).toUriString();
                res.sendRedirect(target);
                return;
            }

            writeSessionCookies(res, dto.accessToken(), dto.refreshToken());

            String target = UriComponentsBuilder.fromUriString(frontendUrl)
                    .queryParam("signUp", dto.signUp())
                    .build()
                    .encode(StandardCharsets.UTF_8)
                    .toUriString();
            res.sendRedirect(target);
            log.info("[CTRL] Redirecting back to frontend with tokens: {}", target);

        } catch (ApiException e) {
            String mode = resolveModeFromState(state);
            if ("android".equals(mode) || "ios".equals(mode) || "app".equals(mode)) {
                String target = UriComponentsBuilder.fromUriString("aura://oauth-callback")
                        .queryParam("error", e.getErrorCode())
                        .queryParam("status", e.getStatus().value())
                        .queryParam("message", e.getMessage())
                        .build()
                        .encode(StandardCharsets.UTF_8)
                        .toUriString();
                res.sendRedirect(target);
                return;
            }
            String target = UriComponentsBuilder.fromUriString(frontendUrl)
                    .path("/auth/error")
                    .queryParam("status", e.getStatus().value())
                    .queryParam("code", e.getErrorCode())
                    .queryParam("message", e.getMessage())
                    .build()
                    .encode(StandardCharsets.UTF_8)
                    .toUriString();

            res.sendRedirect(target);
        } catch (Exception e) {
            String mode = resolveModeFromState(state);
            if ("android".equals(mode) || "ios".equals(mode) || "app".equals(mode)) {
                String deeplink = UriComponentsBuilder.fromUriString("aura://oauth-callback")
                        .queryParam("error", "INTERNAL_SERVER_ERROR")
                        .queryParam("status", 500)
                        .queryParam("message", "서버 낵부 오류가 발생했습니다.")
                        .build()
                        .encode(StandardCharsets.UTF_8)
                        .toUriString();
                res.sendRedirect(deeplink);
                return;
            }
            String target = UriComponentsBuilder.fromUriString(frontendUrl)
                    .path("/auth/error")
                    .queryParam("status", 500)
                    .queryParam("code", "INTERNAL_SERVER_ERROR")
                    .queryParam("message", "서버 낵부 오류가 발생했습니다.")
                    .build()
                    .encode(StandardCharsets.UTF_8)
                    .toUriString();

            res.sendRedirect(target);
        }
    }

    private String resolveAndValidateState(String state) {
        if (state == null || state.isBlank()) {
            throw new ApiException(
                    org.springframework.http.HttpStatus.UNAUTHORIZED,
                    "	state 파라미터가 누락되었습니다.",
                    "INVALID_STATE",
                    "state"
            );
        }
        byte[] decoded;
        try {
            decoded = java.util.Base64.getUrlDecoder().decode(state);
        } catch (IllegalArgumentException e) {
            throw new ApiException(
                    org.springframework.http.HttpStatus.UNAUTHORIZED,
                    "유효하지 않은 state 형식입니다.",
                    "INVALID_STATE",
                    "state"
            );
        }
        String[] parts = new String(decoded, StandardCharsets.UTF_8).split(":", 2);
        if (parts.length != 2) {
            throw new ApiException(
                    org.springframework.http.HttpStatus.UNAUTHORIZED,
                    "state 파싱에 실패했습니다.",
                    "INVALID_STATE",
                    "state"
            );
        }
        String nonce = parts[0];
        String validatedMode = ssoTicketService.consumeStateNonce(nonce);
        if (validatedMode == null) {
            throw new ApiException(
                    org.springframework.http.HttpStatus.UNAUTHORIZED,
                    "state가 유효하지 않거나 만료되었습니다.",
                    "INVALID_STATE",
                    "state"
            );
        }
        return validatedMode;
    }

    private String resolveModeFromState(String state) {
        if (state == null || state.isBlank()) {
            return "web";
        }
        try {
            byte[] decoded = java.util.Base64.getUrlDecoder().decode(state);
            String[] parts = new String(decoded, StandardCharsets.UTF_8).split(":", 2);
            if (parts.length == 2) {
                String nonce = parts[0];
                String storedMode = ssoTicketService.consumeStateNonce(nonce);
                if (storedMode != null) {
                    return storedMode;
                }
            }
        } catch (Exception ignored) {
        }
        return "web";
    }

    @PostMapping("/app/exchange")
    public ResponseEntity<ApiResponse<AppExchangeResponseDto>> exchangeApp(@RequestBody(required = false) AppExchangeRequestDto request) {
        String code = request != null ? request.code() : null;
        var exchanged = ssoTokenExchangeService.exchangeTicket(code);

        AppExchangeResponseDto response = new AppExchangeResponseDto(
                exchanged.accessToken(),
                exchanged.refreshToken(),
                exchanged.signUp(),
                new AuthUserDto(exchanged.email(), exchanged.name())
        );

        return ResponseEntity.ok(ApiResponse.success(response));
    }

    // 리프레시
    @PostMapping("/refresh")
    public ResponseEntity<ApiResponse<AuthRefreshResponseDto>> refresh(
            @CookieValue(value = "refreshToken", required = false) String cookieRt,
            @RequestBody(required = false) AuthTokenRequestDto body,
            HttpServletResponse response
    ) {
        String rt = resolveRefreshToken(cookieRt, body);
        log.info("[REFRESH] called: source={}, hasRt={}",
                tokenSource(cookieRt, body),
                (rt != null));

        if (rt == null) {
            throw new sulhoe.aura.handler.ApiException(
                    org.springframework.http.HttpStatus.BAD_REQUEST,
                    "요청 형식이 올바르지 않습니다.",
                    "MISSING_REFRESH_TOKEN",
                    "refreshToken"
            );
        }

        try{
            var dto = authService.refreshAccessToken(rt); // (회전된 RT 포함)

            // 웹 호환: 쿠키도 항상 갱신해줌(앱은 무시)
            writeSessionCookies(response, dto.accessToken(), dto.refreshToken());

            // 앱 호환: JSON도 내려줌(웹은 안 써도 됨)
            return ResponseEntity.ok(ApiResponse.success(
                    new AuthRefreshResponseDto(dto.accessToken(), dto.refreshToken())
            ));
        } catch (sulhoe.aura.handler.ApiException ex) {
            // 서비스가 401을 던지면 쿠키 정리 헤더를 보장
            if (ex.getStatus().value() == 401) {
                var headers = new HttpHeaders();
                headers.add(HttpHeaders.SET_COOKIE, clearCookie("refreshToken").toString());
                headers.add(HttpHeaders.SET_COOKIE, clearCookie("WEB_SESSION").toString());
                headers.add(HttpHeaders.WWW_AUTHENTICATE,
                        "Bearer error=\"invalid_token\", error_description=\"refresh_expired_or_invalid\"");
                throw new sulhoe.aura.handler.ApiException(
                        org.springframework.http.HttpStatus.UNAUTHORIZED,
                        ex.getMessage(),
                        ex.getErrorCode(),
                        "refreshToken",
                        headers
                );
            }
            throw ex; // 401 외의 ApiException은 그대로
        } catch (RuntimeException e) {
            log.warn("[REFRESH] invalid/expired RT: {}", e.getMessage());

            // 쿠키 제거 + WWW-Authenticate 헤더를 ApiException에 실어 전달
            var headers = new HttpHeaders();
            headers.add(HttpHeaders.SET_COOKIE, clearCookie("refreshToken").toString());
            headers.add(HttpHeaders.SET_COOKIE, clearCookie("WEB_SESSION").toString());
            headers.add(HttpHeaders.WWW_AUTHENTICATE,
                    "Bearer error=\"invalid_token\", error_description=\"refresh_expired_or_invalid\"");

            throw new sulhoe.aura.handler.ApiException(
                    org.springframework.http.HttpStatus.UNAUTHORIZED,
                    "토큰이 유효하지 않거나 만료되었습니다.",
                    "INVALID_REFRESH_TOKEN",
                    "refreshToken",
                    headers
            );
        }
    }

    @GetMapping("/sso/bridge")
    public ResponseEntity<String> ssoBridge(@RequestParam("code") String ticket) {
        // Legacy/WebView bridge: 네이티브 앱은 /auth/app/exchange 를 사용한다.
        var exchanged = ssoTokenExchangeService.exchangeTicket(ticket);

        String target = UriComponentsBuilder.fromUriString(frontendUrl)
                .queryParam("embed", "app")
                .queryParam("signUp", exchanged.signUp())
                .build(true).toUriString();

        String safeAttr = HtmlUtils.htmlEscape(target);
        String safeJs   = target.replace("\\","\\\\").replace("<","\\x3C")
                .replace(">","\\x3E").replace("&","\\x26")
                .replace("\"","\\\"").replace("'","\\'");

        String html = """
    <!doctype html>
    <meta name="color-scheme" content="light dark">
    <title>Signing you in…</title>
    <meta http-equiv="refresh" content="0; url=%s">
    <script>try{window.location.replace('%s')}catch(e){location.href='%s'}</script>
    """.formatted(safeAttr, safeJs, safeJs);

        return ResponseEntity.ok()
                .header(HttpHeaders.SET_COOKIE, webSessionCookie(exchanged.accessToken()).toString())
                .header(HttpHeaders.SET_COOKIE, refreshCookie(exchanged.refreshToken()).toString())
                .header(HttpHeaders.CACHE_CONTROL, "no-store")
                .contentType(MediaType.TEXT_HTML)
                .body(html);
    }

    /**
     * 로그아웃 처리
     * - DB에서 refreshToken 무효화
     * - refreshToken과 WEB_SESSION 쿠키 삭제
     * - 웹/앱 모두 사용 가능
     */
    @PostMapping("/logout")
    public ResponseEntity<ApiResponse<Void>> logout(
            @CookieValue(value = "refreshToken", required = false) String cookieRt,
            @RequestBody(required = false) AuthTokenRequestDto body,
            HttpServletResponse response
    ) {
        String refreshToken = resolveRefreshToken(cookieRt, body);
        log.info("[LOGOUT] called: source={}, hasRt={}", tokenSource(cookieRt, body), (refreshToken != null));

        // 리프레시 토큰이 있으면 DB에서 무효화
        if (refreshToken != null) {
            try {
                authService.revokeRefreshToken(refreshToken);
                log.info("[LOGOUT] Successfully revoked refresh token");
            } catch (Exception e) {
                log.warn("[LOGOUT] Failed to revoke refresh token: {}", e.getMessage());
                // 실패해도 쿠키는 삭제
            }
        }

        // 쿠키 삭제
        clearAuthCookies(response);

        return ResponseEntity.ok(ApiResponse.success(null));
    }
}
