package sulhoe.aura.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.test.context.ActiveProfiles;

import com.google.firebase.FirebaseApp;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class SecurityTestConfig {

    @MockBean
    private FirebaseApp firebaseApp;

    @Autowired
    private JwtAuthenticationFilter jwtAuthenticationFilter;

    @Autowired
    private SecurityFilterChain securityFilterChain;

    @Test
    void contextLoads() {
        assertThat(firebaseApp).isNotNull();
    }

    @Test
    void jwtAuthenticationFilterExists() {
        assertThat(jwtAuthenticationFilter).isNotNull();
    }

    @Test
    void securityFilterChainLoaded() {
        assertThat(securityFilterChain).isNotNull();
    }
}