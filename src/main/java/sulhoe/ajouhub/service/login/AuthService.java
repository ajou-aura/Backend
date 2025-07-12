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

        User user = userRepository.findByEmail(info.email())
                .map(u -> {
                    u.setName(info.name());
                    log.debug("[AUTH] Existing user, updated name to {}", info.name());
                    return u;
                })
                .orElseGet(() -> {
                    User created = new User(
                            info.name(), info.email(), Set.of(info.department())
                    );
                    log.debug("[AUTH] New user created: id={} email={}", created.getId(), created.getEmail());
                    return created;
                });


        String access  = jwtTokenProvider.createAccessToken(user.getEmail(), user.getName());
        String refresh = jwtTokenProvider.createRefreshToken(user.getEmail());
        log.debug("[AUTH] Tokens issued: access.length={} refresh.length={}",
                access.length(), refresh.length());

        // 리프레시 토큰 저장 및 회전 초기화
        user.setRefreshToken(refresh);
        userRepository.save(user);

        return new LoginResponseDto(access, refresh);
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

        return new LoginResponseDto(newAccess, newRefresh);
    }
}
