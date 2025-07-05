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
                    User created = userRepository.save(new User(
                            info.name(), info.email(), info.department()
                    ));
                    log.debug("[AUTH] New user created: id={} email={}", created.getId(), created.getEmail());
                    return created;
                });


        String access  = jwtTokenProvider.createAccessToken(user.getEmail(), user.getName(), user.getDepartment());
        String refresh = jwtTokenProvider.createRefreshToken(user.getEmail());
        log.debug("[AUTH] Tokens issued: access.length={} refresh.length={}",
                access.length(), refresh.length());

        // TODO: 리프레시 토큰 저장
        return new LoginResponseDto(access, refresh);
    }

    @Transactional
    public String refreshAccessToken(String refreshToken) {
        log.debug("[AUTH] refreshAccessToken, refreshToken={}", refreshToken);

        if (!jwtTokenProvider.validateToken(refreshToken)) {
            throw new RuntimeException("Invalid refresh token");
        }
        String email = jwtTokenProvider.getEmail(refreshToken);
        User user = userRepository.findByEmail(email)
                .orElseThrow(() -> new RuntimeException("User not found: " + email));
        log.debug("[AUTH] Refreshing for user: {}", email);

        String newAccess = jwtTokenProvider.createAccessToken(
                user.getEmail(), user.getName(), user.getDepartment());
        log.debug("[AUTH] New access token length: {}", newAccess.length());

        return newAccess;
    }
}
