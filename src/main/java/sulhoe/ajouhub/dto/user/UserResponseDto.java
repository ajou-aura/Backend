package sulhoe.ajouhub.dto.user;

import java.util.Set;

public record UserResponseDto(
        String name,
        String email,
        Set<String> departments
) {
}
