package sulhoe.aura.config;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.web.savedrequest.NullRequestCache;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;
import org.springframework.security.web.servlet.util.matcher.MvcRequestMatcher;
import org.springframework.security.web.util.matcher.RequestMatcher;
import org.springframework.web.filter.ForwardedHeaderFilter;
import org.springframework.web.servlet.handler.HandlerMappingIntrospector;

@Configuration
@RequiredArgsConstructor
public class SecurityConfig {

    private final JwtAuthenticationFilter jwtFilter;

    @Bean
    PasswordEncoder passwordEncoder() {
        return PasswordEncoderFactories.createDelegatingPasswordEncoder();
    }

    @Bean HandlerMappingIntrospector introspector() { return new HandlerMappingIntrospector(); }

    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        var repo = CookieCsrfTokenRepository.withHttpOnlyFalse();
        // ⬇교차 사이트에서 쿠키가 전송되도록
        repo.setCookieCustomizer(c -> c
                .sameSite("None")
                .secure(true)
                .path("/"));

        return http
                // CORS는 WebConfig.addCorsMappings에서 설정 → 여기선 활성화만
                .cors(Customizer.withDefaults())

                // CSRF: 쿠키/쿠키 기반이면 활성 권장. 다만 인증/리프레시 등은 예외 처리
                .csrf(csrf -> csrf
                        .csrfTokenRepository(repo)
                        .ignoringRequestMatchers(
                                new MvcRequestMatcher(introspector(), "/api/auth/**"),// 로그인/콜백/리프레시/SSO 브리지 등
                                (HttpServletRequest req) -> { // Authorization: Bearer면 CSRF 제외
                                    String auth = req.getHeader("Authorization");
                                    return auth != null && auth.startsWith("Bearer ");
                                }
                        )
                )

                // 세션 비사용(JWT)
                .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.STATELESS))

                // 요청 캐시 비활성(401→리프레시 재시도에 유리)
                .requestCache(rc -> rc.requestCache(new NullRequestCache()))

                // 권한 설정 (전역 /api 프리픽스 기준)
                .authorizeHttpRequests(auth -> auth
                        // Preflight
                        .requestMatchers(HttpMethod.OPTIONS, "/**").permitAll()

                        // 공개 엔드포인트
                        .requestMatchers(
                                "/api/auth/**",
                                "/api/notices/**",
                                "/favicon.ico", "/", "/index.html", "/assets/**", "/static/**", "/health"
                        ).permitAll()

                        // 그 외 보호
                        .anyRequest().authenticated()
                )

                // 401/403 응답 표준화(프론트 만료 감지용)
                .exceptionHandling(ex -> ex
                        .authenticationEntryPoint((req, res, e) -> {
                            res.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                            res.setHeader("WWW-Authenticate",
                                    "Bearer error=\"invalid_token\", error_description=\"expired\"");
                            res.setContentType("application/json");
                            res.getWriter().write("{\"code\":\"ACCESS_TOKEN_EXPIRED\",\"message\":\"Unauthorized\"}");
                        })
                        .accessDeniedHandler((req, res, e) -> {
                            res.setStatus(HttpServletResponse.SC_FORBIDDEN);
                            res.setContentType("application/json");
                            res.getWriter().write("{\"code\":\"FORBIDDEN\",\"message\":\"Forbidden\"}");
                        })
                )

                // JWT 필터 삽입 (UsernamePasswordAuthenticationFilter 앞)
                .addFilterBefore(jwtFilter, UsernamePasswordAuthenticationFilter.class)

                // 불필요한 기본 인증/폼 로그인 비활성
                .httpBasic(AbstractHttpConfigurer::disable)
                .formLogin(AbstractHttpConfigurer::disable)

                .build();
    }

    // 리버스 프록시(HTTPS Termination) 환경에서 X-Forwarded-* 신뢰 → Secure 쿠키/리다이렉트 판별 정확성 향상
    @Bean
    ForwardedHeaderFilter forwardedHeaderFilter() {
        return new ForwardedHeaderFilter();
    }

    @Bean
    AuthenticationManager authenticationManager(AuthenticationConfiguration cfg) throws Exception {
        return cfg.getAuthenticationManager();
    }
}
