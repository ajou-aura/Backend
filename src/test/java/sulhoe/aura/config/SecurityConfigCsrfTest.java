package sulhoe.aura.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.firebase.FirebaseApp;
import jakarta.servlet.http.Cookie;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import sulhoe.aura.entity.Role;
import sulhoe.aura.dto.user.DepartmentRequestDto;
import sulhoe.aura.service.login.UserService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@ActiveProfiles("test")
class SecurityConfigCsrfTest {

    private static final String EMAIL = "user@ajou.ac.kr";
    private static final String NAME = "Aura User";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JwtTokenProvider jwtTokenProvider;

    @MockBean
    private FirebaseApp firebaseApp;

    @MockBean
    private UserService userService;

    @Test
    void csrfEndpointAccessibleWithoutAuthentication() throws Exception {
        MvcResult result = mockMvc.perform(get("/api/auth/csrf"))
                .andExpect(status().isNoContent())
                .andExpect(header().exists("X-CSRF-TOKEN"))
                .andReturn();

        assertThat(result.getResponse().getCookie("XSRF-TOKEN")).isNotNull();
    }

    @Test
    void cookieAuthenticatedPostWithoutCsrfReturnsForbidden() throws Exception {
        mockMvc.perform(post("/api/user/departments")
                        .cookie(webSessionCookie())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new DepartmentRequestDto("software"))))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.code").value("CSRF_FAILED"));

        verifyNoInteractions(userService);
    }

    @Test
    void cookieAuthenticatedPostWithCsrfReturnsOk() throws Exception {
        MvcResult csrfResult = mockMvc.perform(get("/api/auth/csrf"))
                .andExpect(status().isNoContent())
                .andReturn();

        Cookie csrfCookie = csrfResult.getResponse().getCookie("XSRF-TOKEN");
        String csrfToken = csrfResult.getResponse().getHeader("X-CSRF-TOKEN");

        assertThat(csrfCookie).isNotNull();
        assertThat(csrfToken).isNotBlank();

        mockMvc.perform(post("/api/user/departments")
                        .cookie(webSessionCookie(), csrfCookie)
                        .header("X-XSRF-TOKEN", csrfToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new DepartmentRequestDto("software"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("success"));

        verify(userService).addDepartmentByEmail(EMAIL, "software");
    }

    @Test
    void bearerAuthenticatedPostWithoutCsrfReturnsOk() throws Exception {
        mockMvc.perform(post("/api/user/departments")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + jwtTokenProvider.createAccessToken(EMAIL, NAME, Role.USER))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new DepartmentRequestDto("computer"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("success"));

        verify(userService).addDepartmentByEmail(EMAIL, "computer");
    }

    private Cookie webSessionCookie() {
        return new Cookie("WEB_SESSION", jwtTokenProvider.createAccessToken(EMAIL, NAME, Role.USER));
    }
}
