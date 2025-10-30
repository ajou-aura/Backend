package sulhoe.aura.config;

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

/**
 * JWT 인증 필터
 *
 * - 웹(브라우저) 요청: HttpOnly 쿠키(WEB_SESSION) 기반 인증 (쿠키 우선)
 * - 앱/툴 요청: Authorization: Bearer 헤더 보조
 *
 * 주의:
 * - 여기서는 유효하지 않은 토큰일 때 Authentication을 건드리지 않고 체인을 계속 흘립니다.
 *   실제 401 응답 처리는 Security 설정의 AuthenticationEntryPoint에서 담당하도록 두는 것이 깔끔합니다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JwtAuthenticationFilter extends OncePerRequestFilter {

    private final JwtTokenProvider jwt;

    private static final String HEADER_AUTH   = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";
    private static final String WEB_COOKIE    = "WEB_SESSION";

    @Override
    protected void doFilterInternal(HttpServletRequest req,
                                    HttpServletResponse res,
                                    FilterChain chain) throws ServletException, IOException {

        // 이미 인증이 있는 경우 중복 설정 방지
        if (SecurityContextHolder.getContext().getAuthentication() == null) {
            String token = resolveToken(req);

            if (token != null) {
                // JwtTokenProvider#validateToken 은 내부에서 파싱/검증하고 boolean만 반환
                if (jwt.validateToken(token)) {
                    String email = jwt.getEmail(token);
                    String name  = jwt.getName(token);

                    Map<String, String> principal = Map.of(
                            "email", email,
                            "name",  name
                    );

                    List<SimpleGrantedAuthority> roles =
                            List.of(new SimpleGrantedAuthority("ROLE_USER"));

                    var authentication = new UsernamePasswordAuthenticationToken(
                            principal,   // principal
                            token,       // credentials (optional)
                            roles        // authorities
                    );
                    authentication.setDetails(new WebAuthenticationDetailsSource().buildDetails(req));
                    SecurityContextHolder.getContext().setAuthentication(authentication);

                    log.info("JWT 인증 성공: {} / {}", email, name);
                } else {
                    // 유효하지 않은 토큰이면 인증 생략 (EntryPoint가 최종 401 처리)
                    log.warn("[FILTER] JWT 토큰 검증 실패. 요청 URI: {}", req.getRequestURI());
                }
            } else {
                log.debug("[FILTER] 인증 토큰 없음. 요청 URI: {}", req.getRequestURI());
            }
        }

        chain.doFilter(req, res);
    }

    /**
     * 토큰 추출 규칙:
     * 1) WEB_SESSION 쿠키 우선(웹/웹뷰)
     * 2) 없으면 Authorization: Bearer 헤더(앱/툴)
     */
    private String resolveToken(HttpServletRequest req) {
        // 1) 쿠키(WEB_SESSION) 우선
        Cookie[] cookies = Optional.ofNullable(req.getCookies()).orElse(new Cookie[0]);
        String fromCookie = Arrays.stream(cookies)
                .filter(c -> WEB_COOKIE.equals(c.getName()))
                .map(Cookie::getValue)
                .findFirst()
                .orElse(null);

        if (fromCookie != null && !fromCookie.isBlank()) {
            log.debug("[FILTER] JWT from cookie {}", WEB_COOKIE);
            return fromCookie;
        }

        // 2) 헤더(Bearer) 보조
        String header = req.getHeader(HEADER_AUTH);
        if (header != null && header.startsWith(BEARER_PREFIX)) {
            log.debug("[FILTER] JWT from Authorization header");
            return header.substring(BEARER_PREFIX.length());
        }

        return null;
    }
}
