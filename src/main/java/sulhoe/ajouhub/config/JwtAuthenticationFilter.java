package sulhoe.ajouhub.config;

import io.jsonwebtoken.JwtException;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import sulhoe.ajouhub.config.JwtTokenProvider;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class JwtAuthenticationFilter extends OncePerRequestFilter {
    private final JwtTokenProvider jwt;
    private static final String HEADER = "Authorization";
    private static final String PREFIX = "Bearer ";

    @Override
    protected void doFilterInternal(HttpServletRequest req,
                                    HttpServletResponse res,
                                    FilterChain chain)
            throws ServletException, IOException {

        String header = req.getHeader(HEADER);
        log.trace("[FILTER] Authorization header: {}", header);

        if (header != null && header.startsWith(PREFIX)) {
            String token = header.substring(PREFIX.length());
            try {
                if (jwt.validateToken(token)) {
                    // JWT에서 클레임 꺼내기
                    String email = jwt.getEmail(token);
                    String name  = jwt.getName(token);
                    log.debug("[FILTER] Authenticated user: {} / {}", email, name);

                    // principal 에 Map 형태로 담아둬도 되고, 필요하면 별도 DTO를 만들어도 좋습니다.
                    Map<String,String> principal = Map.of(
                            "email", email,
                            "name",  name
                    );

                    // 권한은 ROLE_USER 하나만 줍니다.
                    List<SimpleGrantedAuthority> auths =
                            List.of(new SimpleGrantedAuthority("ROLE_USER"));

                    var auth = new UsernamePasswordAuthenticationToken(
                            principal,   // principal
                            token,       // credentials (optional)
                            auths        // authorities
                    );
                    auth.setDetails(new WebAuthenticationDetailsSource().buildDetails(req));
                    SecurityContextHolder.getContext().setAuthentication(auth);

                    log.debug("JWT 인증 성공: {} / {} /", email, name);
                }
            } catch (JwtException e) {
                log.warn("[FILTER] JWT 검증 실패: {}", e.getMessage());
            }
        }
        else {
            log.trace("[FILTER] No Bearer token, skipping authentication");
        }

        chain.doFilter(req, res);
    }
}
