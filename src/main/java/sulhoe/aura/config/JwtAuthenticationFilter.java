package sulhoe.aura.config;

import io.jsonwebtoken.JwtException;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.Cookie;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class JwtAuthenticationFilter extends OncePerRequestFilter {
    private final JwtTokenProvider jwt;
    private static final String HEADER = "Authorization";
    private static final String PREFIX = "Bearer ";
    private static final String WEB_COOKIE = "WEB_SESSION";

    @Override
    protected void doFilterInternal(HttpServletRequest req,
                                    HttpServletResponse res,
                                    FilterChain chain)
            throws ServletException, IOException {

        String token = null;
        String header = req.getHeader(HEADER);
        log.trace("[FILTER] Authorization header: {}", header);

        if (header != null && header.startsWith(PREFIX)) {
            token = header.substring(PREFIX.length());
        } else {
            // 2) 대안: 헤더가 없으면 WEB_SESSION 쿠키에서 JWT 읽기(웹/웹뷰)
            Cookie[] cookies = Optional.ofNullable(req.getCookies()).orElse(new Cookie[0]);
            token = Arrays.stream(cookies)
                    .filter(c -> WEB_COOKIE.equals(c.getName()))
                    .map(Cookie::getValue)
                    .findFirst().orElse(null);
        }
        if (token != null) {
            try {
                if (jwt.validateToken(token)) {
                    // JWT에서 클레임 꺼내기
                    String email = jwt.getEmail(token);
                    String name  = jwt.getName(token);
                    log.debug("[FILTER] Authenticated user: {} / {}", email, name);

                    Map<String,String> principal = Map.of("email", email,"name", name);

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
