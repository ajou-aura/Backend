package sulhoe.aura.dto.user;
import org.springframework.lang.Nullable;

public record OAuthUserInfo(
        String name,
        String email,
        @Nullable String department
) {}

