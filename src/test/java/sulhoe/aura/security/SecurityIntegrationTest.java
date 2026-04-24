package sulhoe.aura.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.firebase.FirebaseApp;
import jakarta.servlet.http.Cookie;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.web.context.WebApplicationContext;
import sulhoe.aura.config.JwtTokenProvider;
import sulhoe.aura.entity.Role;
import sulhoe.aura.entity.User;
import sulhoe.aura.repository.UserRepository;
import sulhoe.aura.service.firebase.PushNotificationService;
import sulhoe.aura.service.login.AuthService;
import sulhoe.aura.service.login.GoogleOAuthService;
import sulhoe.aura.service.login.SsoTicketService;
import sulhoe.aura.service.login.UserService;

import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.task.scheduling.enabled=false"
)
@ActiveProfiles("test")
class SecurityIntegrationTest {

    private static final String USER_EMAIL = "user@ajou.ac.kr";
    private static final String USER_NAME = "Aura User";
    private static final String ADMIN_EMAIL = "admin@test.com";
    private static final String ADMIN_NAME = "Aura Admin";

    @Autowired
    private WebApplicationContext webApplicationContext;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JwtTokenProvider jwtTokenProvider;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private SsoTicketService ssoTicketService;

    @MockBean
    private FirebaseApp firebaseApp;

    @MockBean
    private PushNotificationService pushNotificationService;

    @MockBean
    private UserService userService;

    @MockBean
    private GoogleOAuthService googleOAuthService;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = webAppContextSetup(webApplicationContext)
                .apply(SecurityMockMvcConfigurers.springSecurity())
                .build();

        userRepository.deleteAll();
        clearSsoStores();
        reset(pushNotificationService, userService, googleOAuthService);
    }

    @Test
    void cookieAuthenticatedPostToDepartmentsRequiresCsrfToken() throws Exception {
        mockMvc.perform(post("/api/user/departments")
                        .cookie(webSessionCookie(USER_EMAIL, USER_NAME, Role.USER))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "department": "software"
                                }
                                """))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.code").value("CSRF_FAILED"))
                .andExpect(jsonPath("$.message").value("Forbidden"));

        verifyNoInteractions(userService);
    }

    @Test
    void bearerTokenCanExchangeAppTicketWithoutCsrfToken() throws Exception {
        persistUser(USER_EMAIL, USER_NAME, Role.USER, AuthService.hashSha256("seed-refresh"));
        String ticket = ssoTicketService.issue(USER_EMAIL, USER_NAME, false);

        mockMvc.perform(post("/api/auth/app/exchange")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + jwtTokenProvider.createAccessToken(USER_EMAIL, USER_NAME, Role.USER))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Map.of("code", ticket))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("success"))
                .andExpect(jsonPath("$.data.accessToken").isNotEmpty())
                .andExpect(jsonPath("$.data.refreshToken").isNotEmpty())
                .andExpect(jsonPath("$.data.user.email").value(USER_EMAIL));
    }

    @Test
    void regularUserGetsForbiddenOnAdminEndpoint() throws Exception {
        MvcResult csrfResult = mockMvc.perform(get("/api/auth/csrf"))
                .andExpect(status().isNoContent())
                .andReturn();
        Cookie csrfCookie = csrfResult.getResponse().getCookie("XSRF-TOKEN");
        String csrfToken = csrfResult.getResponse().getHeader("X-CSRF-TOKEN");

        mockMvc.perform(post("/api/admin/push/topic")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + jwtTokenProvider.createAccessToken(USER_EMAIL, USER_NAME, Role.USER))
                        .cookie(csrfCookie)
                        .header("X-XSRF-TOKEN", csrfToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "topic": "notices",
                                  "type": "general",
                                  "title": "hello",
                                  "link": "https://example.com/notices/1"
                                }
                                """))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.code").value("FORBIDDEN"))
                .andExpect(jsonPath("$.message").value("Forbidden"));

        verifyNoInteractions(pushNotificationService);
    }

    @Test
    void adminUserCanAccessAdminEndpoint() throws Exception {
        MvcResult csrfResult = mockMvc.perform(get("/api/auth/csrf"))
                .andExpect(status().isNoContent())
                .andReturn();
        Cookie csrfCookie = csrfResult.getResponse().getCookie("XSRF-TOKEN");
        String csrfToken = csrfResult.getResponse().getHeader("X-CSRF-TOKEN");

        mockMvc.perform(post("/api/admin/push/topic")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + jwtTokenProvider.createAccessToken(ADMIN_EMAIL, ADMIN_NAME, Role.ADMIN))
                        .cookie(csrfCookie)
                        .header("X-XSRF-TOKEN", csrfToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "topic": "notices",
                                  "type": "general",
                                  "title": "hello",
                                  "link": "https://example.com/notices/1"
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("success"));

        verify(pushNotificationService)
                .sendToTopic("notices", "general", "hello", "https://example.com/notices/1");
    }

    @Test
    void refreshCycleRotatesTokenAndStoresOnlyHashedCopy() throws Exception {
        String rawRefreshToken = jwtTokenProvider.createRefreshToken(USER_EMAIL);
        persistUser(USER_EMAIL, USER_NAME, Role.USER, AuthService.hashSha256(rawRefreshToken));
        Thread.sleep(1_100);

        MvcResult csrfResult = mockMvc.perform(get("/api/auth/csrf"))
                .andExpect(status().isNoContent())
                .andReturn();

        Cookie csrfCookie = csrfResult.getResponse().getCookie("XSRF-TOKEN");
        String csrfToken = csrfResult.getResponse().getHeader("X-CSRF-TOKEN");

        assertThat(csrfCookie).isNotNull();
        assertThat(csrfToken).isNotBlank();

        MvcResult result = mockMvc.perform(post("/api/auth/refresh")
                        .cookie(csrfCookie)
                        .header("X-XSRF-TOKEN", csrfToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Map.of("refreshToken", rawRefreshToken))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("success"))
                .andExpect(jsonPath("$.data.accessToken").isNotEmpty())
                .andExpect(jsonPath("$.data.refreshToken").isNotEmpty())
                .andReturn();

        String rotatedRefreshToken = responseData(result).path("refreshToken").asText();
        User savedUser = userRepository.findByEmail(USER_EMAIL).orElseThrow();

        assertThat(result.getResponse().getHeaders(HttpHeaders.SET_COOKIE)).hasSize(2);
        assertThat(rotatedRefreshToken).isNotEqualTo(rawRefreshToken);
        assertThat(savedUser.getRefreshToken()).isEqualTo(AuthService.hashSha256(rotatedRefreshToken));
        assertThat(savedUser.getRefreshToken()).isNotEqualTo(rotatedRefreshToken);

        mockMvc.perform(post("/api/auth/refresh")
                        .cookie(csrfCookie)
                        .header("X-XSRF-TOKEN", csrfToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Map.of("refreshToken", rawRefreshToken))))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.data.errors[0].code").value("REFRESH_TOKEN_MISMATCH"));
    }

    @Test
    void ssoTicketBoundsRejectTicket10001AndCleanupReclaimsExpiredCapacity() throws Exception {
        for (int i = 0; i < 10_000; i++) {
            ssoTicketService.issue("user" + i + "@test.com", "User" + i, false);
        }

        assertThatThrownBy(() -> ssoTicketService.issue("overflow@test.com", "Overflow User", false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Ticket store at capacity");

        expireOneStoredTicket();

        assertThat(ssoTicketService.issue("recovered@test.com", "Recovered User", false)).isNotBlank();
    }

    @Test
    void replayedOauthStateIsRejectedWith401ErrorRedirect() throws Exception {
        String nonce = ssoTicketService.generateStateNonce("web");
        String state = Base64.getUrlEncoder().withoutPadding()
                .encodeToString((nonce + ":web").getBytes(StandardCharsets.UTF_8));

        assertThat(ssoTicketService.consumeStateNonce(nonce)).isEqualTo("web");

        mockMvc.perform(get("/api/auth/callback")
                        .param("code", "valid-code")
                        .param("state", state))
                .andExpect(status().is3xxRedirection())
                .andExpect(header().string("Location", org.hamcrest.Matchers.containsString("/auth/error")))
                .andExpect(header().string("Location", org.hamcrest.Matchers.containsString("status=401")))
                .andExpect(header().string("Location", org.hamcrest.Matchers.containsString("code=INVALID_STATE")));

        verifyNoInteractions(googleOAuthService);
    }

    private Cookie webSessionCookie(String email, String name, Role role) {
        return new Cookie("WEB_SESSION", jwtTokenProvider.createAccessToken(email, name, role));
    }

    private User persistUser(String email, String name, Role role, String refreshToken) {
        User user = new User(name, email, Set.of("software"));
        user.setRole(role);
        user.setRefreshToken(refreshToken);
        return userRepository.save(user);
    }

    private JsonNode responseData(MvcResult result) throws Exception {
        return objectMapper.readTree(result.getResponse().getContentAsString()).path("data");
    }

    private void clearSsoStores() {
        ssoStore().clear();
        stateStore().clear();
    }

    @SuppressWarnings("unchecked")
    private ConcurrentHashMap<String, Object> ssoStore() {
        return (ConcurrentHashMap<String, Object>) ReflectionTestUtils.getField(ssoTicketService, "store");
    }

    @SuppressWarnings("unchecked")
    private ConcurrentHashMap<String, Object> stateStore() {
        return (ConcurrentHashMap<String, Object>) ReflectionTestUtils.getField(ssoTicketService, "stateStore");
    }

    private void expireOneStoredTicket() throws Exception {
        Map.Entry<String, Object> entry = ssoStore().entrySet().iterator().next();
        Object currentEntry = entry.getValue();
        Object payload = ReflectionTestUtils.getField(currentEntry, "payload");

        Constructor<?> constructor = currentEntry.getClass().getDeclaredConstructor(payload.getClass(), long.class);
        constructor.setAccessible(true);
        Object expiredEntry = constructor.newInstance(payload, Instant.now().minusSeconds(1).toEpochMilli());
        ssoStore().put(entry.getKey(), expiredEntry);

        ssoTicketService.cleanupExpired();
        assertThat(ssoStore()).hasSize(9_999);
    }
}
