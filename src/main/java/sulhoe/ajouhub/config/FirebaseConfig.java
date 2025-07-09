package sulhoe.ajouhub.config;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

// Firebase와 연결
@Configuration
public class FirebaseConfig {

    // @Value("${firebase.service.account.json}")
    private String serviceAccountJson;

    /* @Bean
    public FirebaseApp firebaseApp() throws Exception {
        if (serviceAccountJson == null || serviceAccountJson.isBlank()) {
            throw new IllegalStateException("FIREBASE_SERVICE_ACCOUNT_JSON is not provided.");
        }

        // 문자열로 되어 있는 JSON -> InputStream 변환
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(serviceAccountJson.getBytes(StandardCharsets.UTF_8))) {

            GoogleCredentials credentials = GoogleCredentials.fromStream(inputStream);
            FirebaseOptions options = FirebaseOptions.builder().setCredentials(credentials).build();

            // 이미 앱이 초기화되어 있지 않다면 새로 초기화
            if (FirebaseApp.getApps().isEmpty()) {
                return FirebaseApp.initializeApp(options);
            } else {
                return FirebaseApp.getInstance();
            }
        }
    }*/
}