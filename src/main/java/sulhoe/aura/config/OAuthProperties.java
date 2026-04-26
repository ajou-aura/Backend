package sulhoe.aura.config;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Platform-specific OAuth configuration properties.
 * Supports web, android, and iOS OAuth clients with fallback logic.
 */
@Slf4j
@Data
@Component
@ConfigurationProperties(prefix = "oauth.google")
public class OAuthProperties {

    private ClientConfig web;
    private ClientConfig android;
    private ClientConfig ios;

    /**
     * Represents OAuth credentials for a specific platform.
     */
    @Data
    public static class ClientConfig {
        private String clientId;
        private String clientSecret;
        private String redirectUri;
    }

    /**
     * Resolves OAuth configuration for the given platform.
     * Fallback chain: ios → android → web
     *
     * @param platform "web", "android", "ios", or "app" (treated as "android")
     * @return ClientConfig for the platform
     * @throws IllegalStateException if no valid configuration found
     */
    public ClientConfig resolve(String platform) {
        String normalized = normalizePlatform(platform);

        ClientConfig config = switch (normalized) {
            case "ios" -> resolveWithFallback(ios, android, web);
            case "android" -> resolveWithFallback(android, web);
            case "web" -> web;
            default -> throw new IllegalArgumentException("Unknown platform: " + platform);
        };

        if (config == null || !isValid(config)) {
            throw new IllegalStateException(
                "No valid OAuth configuration found for platform: " + platform +
                ". Please check oauth.google.* properties in application.yml or .env file."
            );
        }

        log.debug("Resolved OAuth config for platform '{}': clientId={}, redirectUri={}",
            normalized,
            maskClientId(config.getClientId()),
            config.getRedirectUri()
        );

        return config;
    }

    /**
     * Normalizes platform identifier.
     * "app" is treated as "android" for backward compatibility.
     */
    private String normalizePlatform(String platform) {
        if (platform == null || platform.isBlank()) {
            return "web";
        }
        return switch (platform.toLowerCase()) {
            case "app" -> "android";  // backward compatibility
            case "android" -> "android";
            case "ios" -> "ios";
            case "web" -> "web";
            default -> throw new IllegalArgumentException("Unknown platform: " + platform);
        };
    }

    /**
     * Resolves config with fallback chain.
     */
    private ClientConfig resolveWithFallback(ClientConfig primary, ClientConfig... fallbacks) {
        if (isValid(primary)) {
            return primary;
        }
        for (ClientConfig fallback : fallbacks) {
            if (isValid(fallback)) {
                log.warn("Primary OAuth config not available, using fallback. " +
                        "Please check oauth.google.* configuration.");
                return fallback;
            }
        }
        return null;
    }

    /**
     * Checks if ClientConfig has required fields.
     * Note: clientSecret is optional for Android/iOS native OAuth clients.
     */
    private boolean isValid(ClientConfig config) {
        return config != null &&
               config.getClientId() != null && !config.getClientId().isBlank() &&
               config.getRedirectUri() != null && !config.getRedirectUri().isBlank();
        // clientSecret is intentionally not checked - Android/iOS clients don't have secrets
    }

    /**
     * Masks client ID for logging (shows first 10 chars only).
     */
    private String maskClientId(String clientId) {
        if (clientId == null || clientId.length() <= 10) {
            return "***";
        }
        return clientId.substring(0, 10) + "...";
    }

    @PostConstruct
    public void validateOnStartup() {
        log.info("OAuthProperties initialized. Configured platforms: {}",
            Map.of(
                "web", web != null && isValid(web),
                "android", android != null && isValid(android),
                "ios", ios != null && isValid(ios)
            )
        );

        // Validate that at least web is configured
        if (web == null || !isValid(web)) {
            log.error("CRITICAL: Web OAuth configuration is missing or invalid. " +
                     "OAuth functionality may not work correctly.");
        }
    }
}
