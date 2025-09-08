// AuthService.java
package sulhoe.aura.service.login;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import sulhoe.aura.config.JwtTokenProvider;
import sulhoe.aura.dto.login.LoginResponseDto;
import sulhoe.aura.entity.User;
import sulhoe.aura.repository.UserRepository;

import java.util.HashSet;
import java.util.Set;

@Slf4j
@Service
@RequiredArgsConstructor
public class AuthService {
    private final GoogleOAuthService googleOAuthService;
    private final JwtTokenProvider jwtTokenProvider;
    private final UserRepository userRepository;

    @Transactional
    public LoginResponseDto loginWithGoogle(String code) {
        log.debug("[AUTH] loginWithGoogle, code={}", code);

        var info = googleOAuthService.getUserInfoFromCode(code);
        log.debug("[AUTH] Google user info: name={} email={} dept={}",
                info.name(), info.email(), info.department());

        // 이메일로 기존 유저 조회
        var optUser = userRepository.findByEmail(info.email());
        boolean isSignUp = optUser.isEmpty();

        User user;
        if (isSignUp) {
            // 신규 가입
            Set<String> depts = new HashSet<>();
            depts.add(info.department());

            user = new User(
                    info.name(),
                    info.email(),
                    depts
            );
            // 최초 리프레시 토큰 생성
            String initialRefresh = jwtTokenProvider.createRefreshToken(info.email());
            user.setRefreshToken(initialRefresh);
            user = userRepository.save(user);
            log.debug("[AUTH] New user created and refreshToken set");
        } else {
            // 기존 사용자
            user = optUser.get();
            // 이름이 바뀌었을 수도 있으니 업데이트
            user.setName(info.name());
            // RT가 없거나(이전 버전 사용자) 만료 임박이면 교체
            String currentRt = user.getRefreshToken();
            if (currentRt == null || jwtTokenProvider.isExpiringSoon(currentRt, /*sec*/ 3600)) { // 1h 이내 만료면 교체
                String refreshed = jwtTokenProvider.createRefreshToken(info.email());
                user.setRefreshToken(refreshed);
            }
            user = userRepository.save(user);
            log.debug("[AUTH] Existing user updated");
        }

        String access  = jwtTokenProvider.createAccessToken(user.getEmail(), user.getName());
        String refresh = user.getRefreshToken();

        return new LoginResponseDto(access, refresh, isSignUp);
    }

    @Transactional
    public LoginResponseDto refreshAccessToken(String refreshToken) {
        log.debug("[AUTH] refreshAccessToken, refreshToken={}", refreshToken);

        if (!jwtTokenProvider.validateToken(refreshToken)) {
            throw new RuntimeException("Invalid refresh token");
        }
        String email = jwtTokenProvider.getEmail(refreshToken);
        User user = userRepository.findByEmail(email)
                .orElseThrow(() -> new RuntimeException("User not found: " + email));
        log.debug("[AUTH] Refreshing for user: {}", email);

        // DB 저장 토큰과 일치 여부 검사
        if (!refreshToken.equals(user.getRefreshToken())) {
            throw new RuntimeException("Refresh token mismatch");
        }

        // 토큰 회전: 새로운 리프레시 토큰 발급
        String newRefresh = jwtTokenProvider.createRefreshToken(email);

        // === 원자적 회전: old==현재값 일 때만 new로 교체 ===
        int updated = userRepository.rotateRefreshTokenAtomically(email, refreshToken, newRefresh);
        if (updated != 1) { // 0이면 동시 회전/재사용 등으로 이미 값이 바뀐 상황
            throw new RuntimeException("Refresh token race detected (already rotated)");
        }

        String newAccess = jwtTokenProvider.createAccessToken(
                user.getEmail(), user.getName());
        log.debug("[AUTH] New access token length: {}", newAccess.length());

        return new LoginResponseDto(newAccess, newRefresh, false);
    }
}
