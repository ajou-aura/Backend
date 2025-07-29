// AuthService.java
package sulhoe.ajouhub.service.login;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import sulhoe.ajouhub.config.JwtTokenProvider;
import sulhoe.ajouhub.dto.login.LoginResponseDto;
import sulhoe.ajouhub.entity.User;
import sulhoe.ajouhub.repository.UserRepository;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

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
            user = userRepository.save(user);
            log.debug("[AUTH] Existing user updated");
        }


        String access  = jwtTokenProvider.createAccessToken(user.getEmail(), user.getName());
        String refresh = user.getRefreshToken();

        log.debug("[AUTH] Tokens issued: access.length={} refresh.length={}",
                access.length(), refresh.length());

        // 리프레시 토큰 저장 및 회전 초기화
        user.setRefreshToken(refresh);
        userRepository.save(user);

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
        user.setRefreshToken(newRefresh);
        userRepository.save(user);

        String newAccess = jwtTokenProvider.createAccessToken(
                user.getEmail(), user.getName());
        log.debug("[AUTH] New access token length: {}", newAccess.length());

        return new LoginResponseDto(newAccess, newRefresh, false);
    }
}
