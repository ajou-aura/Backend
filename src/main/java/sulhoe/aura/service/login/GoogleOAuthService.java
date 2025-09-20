// GoogleOAuthService.java
package sulhoe.aura.service.login;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.client.WebClient;
import sulhoe.aura.dto.user.OAuthUserInfo;
import sulhoe.aura.handler.ApiException;

import java.util.Locale;

@Service
@RequiredArgsConstructor
public class GoogleOAuthService {
    private final WebClient googleWebClient;
    private final ObjectMapper om;

    @Value("${oauth.google.client-id}")
    private String clientId;
    @Value("${oauth.google.client-secret}")
    private String clientSecret;
    @Value("${oauth.google.redirect-uri}")
    private String redirectUri;

    @Value("${app.auth.allowed-domains:ajou.ac.kr}")
    private String allowedDomainsCsv;

    private boolean isAllowedDomain(String email) {
        if (email == null) return false;
        String lower = email.toLowerCase(Locale.ROOT).trim();
        return Arrays.stream(allowedDomainsCsv.split(","))
                .map(s -> s.toLowerCase(Locale.ROOT).trim())
                .anyMatch(lower::endsWith); // endsWith("@ajou.ac.kr") 포함
    }

    public OAuthUserInfo getUserInfoFromCode(String code) {
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("client_id", clientId);
        form.add("client_secret", clientSecret);
        form.add("code", code);
        form.add("redirect_uri", redirectUri);
        form.add("grant_type", "authorization_code");

        String tokenJson = googleWebClient.post()
                .uri("/token")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .bodyValue(form)
                .retrieve()
                .onStatus(HttpStatusCode::is4xxClientError, resp ->
                        resp.bodyToMono(String.class).map(body ->
                                new ApiException(
                                        HttpStatus.BAD_REQUEST,
                                        "OAUTH_TOKEN_EXCHANGE_FAILED",
                                        "code",
                                        "유효하지 않은 인가 코드입니다."
                                )))
                .onStatus(HttpStatusCode::is4xxClientError, resp ->
                        resp.bodyToMono(String.class).map(body ->
                                new ApiException(
                                        HttpStatus.BAD_GATEWAY,
                                        "OAUTH_GOOGLE_UNAVAILABLE",
                                        null,
                                        "구글 인증 서버에 일시적인 문제가 있습니다."
                                )))
                .bodyToMono(String.class)
                .block();

        JsonNode tokenNode;
        try {
            tokenNode = om.readTree(tokenJson);
        } catch (Exception e) {
            throw new ApiException(HttpStatus.BAD_GATEWAY,
                    "OAUTH_TOKEN_PARSE_FAILED", null, "토큰 응답 파싱에 실패했습니다.");
        }
        String accessToken = tokenNode.path("access_token").asText();
        if (accessToken == null || accessToken.isBlank()) {
            // 구글이 200을 주었지만 access_token이 없는 비정상 응답
            throw new ApiException(HttpStatus.BAD_GATEWAY,
                    "OAUTH_TOKEN_MISSING", null, "유효한 액세스 토큰이 없습니다.");
        }

        String userJson = googleWebClient.get()
                .uri("https://www.googleapis.com/oauth2/v2/userinfo")
                .headers(h -> h.setBearerAuth(accessToken))
                .retrieve()
                .onStatus(HttpStatusCode::is4xxClientError, resp ->
                        resp.bodyToMono(String.class).map(body ->
                                new ApiException(
                                        HttpStatus.UNAUTHORIZED,
                                        "OAUTH_USERINFO_FAILED",
                                        null,
                                        "사용자 정보를 불러올 수 없습니다."
                                )))
                .onStatus(HttpStatusCode::is4xxClientError, resp ->
                        resp.bodyToMono(String.class).map(body ->
                                new ApiException(
                                        HttpStatus.BAD_GATEWAY,
                                        "OAUTH_GOOGLE_UNAVAILABLE",
                                        null,
                                        "구글 사용자 정보 서비스에 문제가 있습니다."
                                )))
                .bodyToMono(String.class)
                .block();

        JsonNode userNode;
        try {
            userNode = om.readTree(userJson);
        } catch (Exception e) {
            throw new ApiException(HttpStatus.BAD_GATEWAY,
                    "OAUTH_USERINFO_PARSE_FAILED", null, "사용자 정보 파싱에 실패했습니다.");
        }
        String name = userNode.get("name").asText();
        String email = userNode.path("email").asText();
        if (email == null || email.isBlank()) {
            throw new ApiException(HttpStatus.BAD_GATEWAY,
                    "OAUTH_EMAIL_MISSING", "email", "구글 계정 이메일을 가져오지 못했습니다.");
        }
        if (!isAllowedDomain(email)) {
            throw new ApiException(HttpStatus.FORBIDDEN,
                    "OAUTH_FORBIDDEN_DOMAIN", "email", "아주대(@ajou.ac.kr) 계정만 로그인할 수 있습니다.");
        }

        return new OAuthUserInfo(name, email, "DefaultDept");
    }
}
