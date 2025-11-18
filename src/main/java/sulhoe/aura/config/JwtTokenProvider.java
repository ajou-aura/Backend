package sulhoe.aura.config;

import io.jsonwebtoken.*;
import io.jsonwebtoken.security.Keys;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.util.Base64;
import java.util.Date;

@Slf4j
@Component
public class JwtTokenProvider {

    @Value("${jwt.key}")
    private String secretKey;              // application.properties 에서 Base64로 인코딩된 키

    private SecretKey signingKey;
    private static final long ACCESS_EXP  = 1000L * 60 * 60 * 24;        // 1일

    public static final long WEB_ACCESS_EXP = 1000L * 60 * 60 * 24;
    public static final long WEB_ACCESS_EXPIRY_SECONDS = WEB_ACCESS_EXP / 1000;

    private static final long REFRESH_EXP = 1000L * 60 * 60 * 24;   // 1일
    public static final long REFRESH_EXPIRY_SECONDS = REFRESH_EXP / 1000;  // 쿠키 maxAge용

    private JwtParser jwtParser; // 시계 오차 허용을 위해 재사용

    @PostConstruct
    public void init() {
        // Base64 디코딩 후 HMAC-SHA 키 생성
        byte[] keyBytes = Base64.getDecoder().decode(secretKey);
        this.signingKey = Keys.hmacShaKeyFor(keyBytes);
        this.jwtParser = Jwts.parserBuilder()
                .setSigningKey(signingKey)
                .setAllowedClockSkewSeconds(60) // 60초 오차 허용
                .build();
    }

    // Access Token: email(subject), name, department 클레임 포함
    public String createAccessToken(String email, String name) {
        long now = System.currentTimeMillis();
        String token = Jwts.builder()
                .setSubject(email)                          // sub = 이메일
                .setIssuedAt(new Date(now))                 // iat
                .setExpiration(new Date(now + ACCESS_EXP))  // exp
                .claim("name", name)                        // 사용자 이름
                .signWith(signingKey, SignatureAlgorithm.HS256)
                .compact();

        log.debug("[JWT-ACCESS] sub={} name={} exp={}", email, name,
                new Date(now + ACCESS_EXP));
        return token;
    }

    // Refresh Token: email(subject) 만 담기
    public String createRefreshToken(String email) {
        long now = System.currentTimeMillis();
        String token =  Jwts.builder()
                .setSubject(email)
                .setIssuedAt(new Date(now))
                .setExpiration(new Date(now + REFRESH_EXP))
                .signWith(signingKey, SignatureAlgorithm.HS256)
                .compact();

        log.debug("[JWT-REFRESH] sub={} exp={}", email, new Date(now + REFRESH_EXP));
        return token;
    }

    // 토큰 유효성 검사
    public boolean validateToken(String token) {
        try {
            jwtParser.parseClaimsJws(token); // 스큐 허용으로 이미 빌드됨
            log.info("[JWT-VALID] token valid, sub={}", getEmail(token));
            return true;
        } catch (JwtException | IllegalArgumentException e) {
            log.warn("[JWT-INVALID] {}", e.getMessage());
            return false;
        }
    }

    public Date getExpiration(String token) {
        return getClaims(token).getExpiration();
    }

    public boolean isExpiringSoon(String token, long withinSeconds) {
        Date exp = getExpiration(token);
        long now = System.currentTimeMillis();
        return exp.getTime() - now <= withinSeconds * 1000L;
    }


    // Claims 꺼내기
    private Claims getClaims(String token) {
        return jwtParser.parseClaimsJws(token).getBody();
    }

    public String getEmail(String token) {
        return getClaims(token).getSubject();
    }

    public String getName(String token) {
        return getClaims(token).get("name", String.class);
    }
}
