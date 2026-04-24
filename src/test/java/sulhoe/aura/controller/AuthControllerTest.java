package sulhoe.aura.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import sulhoe.aura.config.JwtTokenProvider;
import sulhoe.aura.dto.login.AppExchangeRequestDto;
import sulhoe.aura.dto.login.AuthTokenRequestDto;
import sulhoe.aura.dto.login.LoginResponseDto;
import sulhoe.aura.entity.Role;
import sulhoe.aura.handler.GlobalExceptionHandler;
import sulhoe.aura.service.login.AuthService;
import sulhoe.aura.service.login.SsoTicketService;
import sulhoe.aura.service.login.SsoTokenExchangeService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class AuthControllerTest {

    @Mock
    private AuthService authService;

    @Mock
    private JwtTokenProvider jwtTokenProvider;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private SsoTicketService ssoTicketService;
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        ssoTicketService = new SsoTicketService();
        SsoTokenExchangeService ssoTokenExchangeService =
                new SsoTokenExchangeService(ssoTicketService, jwtTokenProvider, authService);

        AuthController controller = new AuthController(
                authService,
                jwtTokenProvider,
                ssoTicketService,
                ssoTokenExchangeService
        );
        ReflectionTestUtils.setField(controller, "frontendUrl", "https://frontend.example.com");
        ReflectionTestUtils.setField(controller, "clientId", "google-client-id");
        ReflectionTestUtils.setField(controller, "redirectUri", "https://backend.example.com/api/auth/callback");

        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    @Test
    void exchangeAppReturnsJsonTokensForValidTicket() throws Exception {
        String ticket = ssoTicketService.issue("user@ajou.ac.kr", "Aura User", true);
        when(authService.findUserRole("user@ajou.ac.kr")).thenReturn(Role.USER);
        when(jwtTokenProvider.createAccessToken("user@ajou.ac.kr", "Aura User", Role.USER)).thenReturn("app-access-token");
        when(authService.ssoRefresh("user@ajou.ac.kr")).thenReturn("app-refresh-token");

        mockMvc.perform(post("/auth/app/exchange")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new AppExchangeRequestDto(ticket))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("success"))
                .andExpect(jsonPath("$.data.accessToken").value("app-access-token"))
                .andExpect(jsonPath("$.data.refreshToken").value("app-refresh-token"))
                .andExpect(jsonPath("$.data.signUp").value(true))
                .andExpect(jsonPath("$.data.user.email").value("user@ajou.ac.kr"))
                .andExpect(jsonPath("$.data.user.name").value("Aura User"));
    }

    @Test
    void exchangeAppRejectsReusedTicket() throws Exception {
        String ticket = ssoTicketService.issue("user@ajou.ac.kr", "Aura User", false);
        when(authService.findUserRole("user@ajou.ac.kr")).thenReturn(Role.USER);
        when(jwtTokenProvider.createAccessToken("user@ajou.ac.kr", "Aura User", Role.USER)).thenReturn("app-access-token");
        when(authService.ssoRefresh("user@ajou.ac.kr")).thenReturn("app-refresh-token");

        mockMvc.perform(post("/auth/app/exchange")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new AppExchangeRequestDto(ticket))))
                .andExpect(status().isOk());

        mockMvc.perform(post("/auth/app/exchange")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new AppExchangeRequestDto(ticket))))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.status").value("error"))
                .andExpect(jsonPath("$.data.errors[0].code").value("INVALID_SSO_TICKET"));
    }

    @Test
    void logoutRevokesRefreshTokenFromRequestBody() throws Exception {
        MvcResult result = mockMvc.perform(post("/auth/logout")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new AuthTokenRequestDto("body-refresh-token"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("success"))
                .andReturn();

        verify(authService).revokeRefreshToken("body-refresh-token");
        assertThat(result.getResponse().getHeaders(HttpHeaders.SET_COOKIE)).hasSize(2);
    }

    @Test
    void refreshReturnsJsonTokensForBodyRefreshToken() throws Exception {
        when(authService.refreshAccessToken("body-refresh-token"))
                .thenReturn(new LoginResponseDto("new-access-token", "new-refresh-token", false));

        MvcResult result = mockMvc.perform(post("/auth/refresh")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new AuthTokenRequestDto("body-refresh-token"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("success"))
                .andExpect(jsonPath("$.data.accessToken").value("new-access-token"))
                .andExpect(jsonPath("$.data.refreshToken").value("new-refresh-token"))
                .andReturn();

        verify(authService).refreshAccessToken("body-refresh-token");
        assertThat(result.getResponse().getHeaders(HttpHeaders.SET_COOKIE)).hasSize(2);
    }

    @Test
    void callbackRejectsInvalidState() throws Exception {
        mockMvc.perform(get("/auth/callback")
                        .param("code", "valid-code")
                        .param("state", "not-valid-base64"))
                .andExpect(status().is3xxRedirection())
                .andExpect(header().string("Location", org.hamcrest.Matchers.containsString("/auth/error")))
                .andExpect(header().string("Location", org.hamcrest.Matchers.containsString("code=INVALID_STATE")));
    }

    @Test
    void callbackRejectsExpiredState() throws Exception {
        byte[] rawState = "nonce:web".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        String expiredState = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(rawState);

        mockMvc.perform(get("/auth/callback")
                        .param("code", "valid-code")
                        .param("state", expiredState))
                .andExpect(status().is3xxRedirection())
                .andExpect(header().string("Location", org.hamcrest.Matchers.containsString("/auth/error")))
                .andExpect(header().string("Location", org.hamcrest.Matchers.containsString("code=INVALID_STATE")));
    }

    @Test
    void callbackAcceptsValidStateAndRedirectsToFrontend() throws Exception {
        String nonce = ssoTicketService.generateStateNonce("web");
        byte[] rawState = (nonce + ":web").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        String validState = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(rawState);

        when(authService.loginWithGoogle("valid-code"))
                .thenReturn(new LoginResponseDto("access-token", "refresh-token", false));

        mockMvc.perform(get("/auth/callback")
                        .param("code", "valid-code")
                        .param("state", validState))
                .andExpect(status().is3xxRedirection())
                .andExpect(header().string("Location", org.hamcrest.Matchers.containsString("https://frontend.example.com")));
    }
}
