// GoogleOAuthService.java
package sulhoe.ajouhub.service.login;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.client.WebClient;
import sulhoe.ajouhub.dto.user.OAuthUserInfo;

import java.util.Locale;

@Service
@RequiredArgsConstructor
public class GoogleOAuthService {
    private final WebClient googleWebClient;
    private final ObjectMapper om;

    @Value("${oauth.google.client-id}")     private String clientId;
    @Value("${oauth.google.client-secret}") private String clientSecret;
    @Value("${oauth.google.redirect-uri}")  private String redirectUri;

    public OAuthUserInfo getUserInfoFromCode(String code) {
        MultiValueMap<String,String> form = new LinkedMultiValueMap<>();
        form.add("client_id", clientId);
        form.add("client_secret", clientSecret);
        form.add("code", code);
        form.add("redirect_uri", redirectUri);
        form.add("grant_type", "authorization_code");

        String tokenJson = googleWebClient.post()
                .uri("/token")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .bodyValue(form)
                .retrieve().bodyToMono(String.class).block();

        try {
            JsonNode tokenNode = om.readTree(tokenJson);
            String accessToken = tokenNode.path("access_token").asText();

            String userJson = googleWebClient.get()
                    .uri("https://www.googleapis.com/oauth2/v2/userinfo")
                    .headers(h -> h.setBearerAuth(accessToken))
                    .retrieve().bodyToMono(String.class).block();

            JsonNode userNode = om.readTree(userJson);
            String name  = userNode.get("name").asText();
            String email = userNode.path("email").asText();
            if (!email.toLowerCase(Locale.ROOT).endsWith("@ajou.ac.kr")) {
                throw new IllegalArgumentException("Only @ajou.ac.kr allowed");
            }
            return new OAuthUserInfo(name, email, "DefaultDept");
        } catch (Exception e) {
            throw new RuntimeException("Google OAuth processing failed", e);
        }
    }
}
