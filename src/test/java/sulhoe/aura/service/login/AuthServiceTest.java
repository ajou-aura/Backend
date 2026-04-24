package sulhoe.aura.service.login;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import sulhoe.aura.config.JwtTokenProvider;
import sulhoe.aura.repository.UserRepository;

@ExtendWith(MockitoExtension.class)
class AuthServiceTest {

    @Mock
    private GoogleOAuthService googleOAuthService;

    @Mock
    private JwtTokenProvider jwtTokenProvider;

    @Mock
    private UserRepository userRepository;

    private AuthService authService;

    @BeforeEach
    void setUp() {
        authService = new AuthService(googleOAuthService, jwtTokenProvider, userRepository);
    }

    @Test
    void hashSha256_returnsDifferentValueFromRawToken() {
        String rawToken = "raw-refresh-token-12345";
        String hash = AuthService.hashSha256(rawToken);
        assertThat(hash).isNotEqualTo(rawToken);
        assertThat(hash).hasSize(44);
    }

    @Test
    void hashSha256_isDeterministic() {
        String rawToken = "raw-refresh-token-12345";
        String hash1 = AuthService.hashSha256(rawToken);
        String hash2 = AuthService.hashSha256(rawToken);
        assertThat(hash1).isEqualTo(hash2);
    }

    @Test
    void hashSha256_differentInputsProduceDifferentHashes() {
        String hash1 = AuthService.hashSha256("token-a");
        String hash2 = AuthService.hashSha256("token-b");
        assertThat(hash1).isNotEqualTo(hash2);
    }
}