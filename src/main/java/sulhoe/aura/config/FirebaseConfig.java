package sulhoe.aura.config;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

@Configuration
@Profile("!test")
public class FirebaseConfig {

    @Value("${firebase.service.account.json}")
    private String serviceAccountJson;

    @Value("${firebase.service.account.json.base64:false}")
    private boolean serviceAccountJsonIsBase64;

    @Bean
    public FirebaseApp firebaseApp() throws Exception {
        if (!FirebaseApp.getApps().isEmpty()) {
            return FirebaseApp.getInstance();
        }
        if (serviceAccountJson == null || serviceAccountJson.isBlank()) {
            throw new IllegalStateException("FIREBASE_SERVICE_ACCOUNT_JSON is not provided.");
        }

        String json = serviceAccountJson;
        if (serviceAccountJsonIsBase64 || looksLikeBase64(serviceAccountJson)) {
            try {
                json = new String(Base64.getDecoder().decode(serviceAccountJson), StandardCharsets.UTF_8);
            } catch (IllegalArgumentException ignore) { /* 그냥 평문으로 사용 */ }
        }

        try (ByteArrayInputStream in = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            FirebaseOptions options = FirebaseOptions.builder()
                    .setCredentials(GoogleCredentials.fromStream(in))
                    .build();
            return FirebaseApp.initializeApp(options);
        }
    }

    private boolean looksLikeBase64(String s) {
        return s.matches("^[A-Za-z0-9+/=\r\n]+$") && s.length() > 100;
    }
}
