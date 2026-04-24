package sulhoe.aura.controller;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.google.firebase.FirebaseApp;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import sulhoe.aura.config.JwtTokenProvider;
import sulhoe.aura.entity.Role;
import sulhoe.aura.service.firebase.PushNotificationService;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@ActiveProfiles("test")
class AdminPushControllerSecurityTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JwtTokenProvider jwtTokenProvider;

    @MockBean
    private FirebaseApp firebaseApp;

    @MockBean
    private PushNotificationService pushNotificationService;

    @Test
    void regularUserGetsForbiddenOnAdminTopicEndpoint() throws Exception {
        String token = jwtTokenProvider.createAccessToken("user@test.com", "Regular User", Role.USER);

        mockMvc.perform(post("/api/admin/push/topic")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
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
    void adminUserCanAccessAdminTopicEndpoint() throws Exception {
        String token = jwtTokenProvider.createAccessToken("admin@test.com", "Admin User", Role.ADMIN);

        mockMvc.perform(post("/api/admin/push/topic")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
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
}
