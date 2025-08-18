// AppConfig.java
package sulhoe.aura.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class AppConfig {

    @Bean
    public WebClient googleWebClient(WebClient.Builder builder) {
        return builder.baseUrl("https://oauth2.googleapis.com").build();
    }
}