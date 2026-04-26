// AuthService.java
package sulhoe.aura.service.login;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import sulhoe.aura.config.JwtTokenProvider;
import sulhoe.aura.dto.login.LoginResponseDto;
import sulhoe.aura.entity.Role;
import sulhoe.aura.entity.User;
import sulhoe.aura.handler.ApiException;
import sulhoe.aura.repository.UserRepository;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;

@Slf4j
@Service
@RequiredArgsConstructor
public class AuthService {
    private static final String REVOKED_REFRESH_TOKEN = "";

    private final GoogleOAuthService googleOAuthService;
    private final JwtTokenProvider jwtTokenProvider;
    private final UserRepository userRepository;

    @Value("${app.admin.emails:}")
    private String adminEmails;

    /**
     * SHA-256 hash a refresh token for storage.
     * Clients still send raw tokens; only the stored copy is hashed.
     */
    public static String hashSha256(String rawToken) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(rawToken.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    @Transactional
    public LoginResponseDto loginWithGoogle(String code, String platform) {
        log.debug("[AUTH] loginWithGoogle, code={}, platform={}", code, platform);

        var info = googleOAuthService.getUserInfoFromCode(code, platform);
        log.debug("[AUTH] Google user info: name={} email={} dept={}",
                info.name(), info.email(), info.department());

        // 이메일로 기존 유저 조회
        var optUser = userRepository.findByEmail(info.email());
        boolean isSignUp = optUser.isEmpty();

        User user;
        // rawToken 변수를 메서드 전체에서 사용할 수 있도록 선언
        final String rawRefreshToken;
        if (isSignUp) {
            // 신규 가입
            Set<String> depts = new HashSet<>();
            if (info.department() != null && !info.department().isBlank()) {
                depts.add(info.department());
            }

            user = new User(
                    info.name(),
                    info.email(),
                    depts
            );
            user.setRole(resolveRole(info.email()));
            // 최초 리프레시 토큰 생성 및 저장 (DB에는 hash, 클라이언트에는 raw)
            rawRefreshToken = jwtTokenProvider.createRefreshToken(info.email());
            user.setRefreshToken(hashSha256(rawRefreshToken));
            user = userRepository.save(user);
            log.debug("[AUTH] New user created and refreshToken set");
        } else {
            // 기존 사용자
            user = optUser.get();
            // 이름이 바뀌었을 수도 있으니 업데이트
            user.setName(info.name());
            user.setRole(resolveRole(info.email()));
            // 항상 새 RT 발급 (기존 만료/임박 체크 제거)
            rawRefreshToken = jwtTokenProvider.createRefreshToken(info.email());
            user.setRefreshToken(hashSha256(rawRefreshToken));
            user = userRepository.save(user);
            log.debug("[AUTH] Existing user updated");
        }

        String access = jwtTokenProvider.createAccessToken(user.getEmail(), user.getName(), user.getRole());
        // 클라이언트에게는 raw token 반환 (DB에는 hash 저장됨)
        return new LoginResponseDto(access, rawRefreshToken, isSignUp);
    }

    @Transactional
    public LoginResponseDto refreshAccessToken(String refreshToken) {
        log.debug("[AUTH] refreshAccessToken, refreshToken=***");

        if (!jwtTokenProvider.validateToken(refreshToken)) {
            throw new ApiException(
                    HttpStatus.UNAUTHORIZED,
                    "토큰이 유효하지 않거나 만료되었습니다.",
                    "INVALID_REFRESH_TOKEN",
                    "refreshToken"
            );
        }
        String email = jwtTokenProvider.getEmail(refreshToken);
        User user = userRepository.findByEmail(email).orElseThrow(() ->
                new ApiException(HttpStatus.NOT_FOUND, "대상을 찾을 수 없습니다.", "NOT_FOUND", "email"));
        log.debug("[AUTH] Refreshing for user: {}", email);

        // DB 저장 토큰과 일치 여부 검사
        if (!hashSha256(refreshToken).equals(user.getRefreshToken())) {
            throw new ApiException(
                    HttpStatus.UNAUTHORIZED,
                    "리프레시 토큰이 일치하지 않습니다.",
                    "REFRESH_TOKEN_MISMATCH",
                    "refreshToken"
            );
        }

        // 토큰 회전: 새로운 리프레시 토큰 발급
        String newRefresh = jwtTokenProvider.createRefreshToken(email);

        // === 원자적 회전: old==현재값 일 때만 new로 교체 ===
        int updated = userRepository.rotateRefreshTokenAtomically(email, hashSha256(refreshToken), hashSha256(newRefresh));
        if (updated != 1) { // 0이면 동시 회전/재사용 등으로 이미 값이 바뀐 상황
            throw new ApiException(
                    HttpStatus.UNAUTHORIZED,
                    "리프레시 토큰이 이미 회전되었습니다.",
                    "REFRESH_TOKEN_RACE",
                    "refreshToken"
            );
        }

        String newAccess = jwtTokenProvider.createAccessToken(
                user.getEmail(), user.getName(), user.getRole());
        log.debug("[AUTH] New access token length: {}", newAccess.length());

        return new LoginResponseDto(newAccess, newRefresh, false);
    }

    @Transactional
    public String ssoRefresh(String email) {
        var user = userRepository.findByEmail(email).orElseThrow(() ->
                new ApiException(HttpStatus.NOT_FOUND, "대상을 찾을 수 없습니다.", "NOT_FOUND", "email"));

        String newRefresh = jwtTokenProvider.createRefreshToken(email);
        // 단일 RT 구조: 새 RT로 교체(기존 RT 무효화)
        user.setRefreshToken(hashSha256(newRefresh));
        userRepository.save(user);

        log.debug("[AUTH] Issued new refresh for web SSO, user={}", email);
        return newRefresh;
    }

    /**
     * 리프레시 토큰 무효화 (로그아웃용)
     * DB에 저장된 리프레시 토큰을 빈 값으로 교체해 재사용을 막는다.
     */
    @Transactional
    public void revokeRefreshToken(String refreshToken) {
        log.debug("[AUTH] revokeRefreshToken called");

        if (refreshToken == null || refreshToken.isBlank()) {
            return;
        }

        String email;
        try {
            email = jwtTokenProvider.getEmail(refreshToken);
        } catch (Exception e) {
            log.warn("[AUTH] Failed to extract email from token: {}", e.getMessage());
            return;
        }

        int updated = userRepository.rotateRefreshTokenAtomically(email, hashSha256(refreshToken), REVOKED_REFRESH_TOKEN);
        if (updated == 1) {
            log.info("[AUTH] Revoked refresh token for user: {}", email);
        } else {
            log.debug("[AUTH] Token mismatch, already rotated or revoked");
        }
    }

    @Transactional(readOnly = true)
    public Role findUserRole(String email) {
        return userRepository.findByEmail(email)
                .map(User::getRole)
                .orElse(Role.USER);
    }

    private Role resolveRole(String email) {
        if (!StringUtils.hasText(email) || adminEmails == null) {
            return Role.USER;
        }

        return Arrays.stream(adminEmails.split(","))
                .map(String::trim)
                .filter(StringUtils::hasText)
                .anyMatch(candidate -> candidate.equalsIgnoreCase(email))
                ? Role.ADMIN
                : Role.USER;
    }
}
