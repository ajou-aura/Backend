package sulhoe.aura.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;
import sulhoe.aura.dto.ApiResponse;
import sulhoe.aura.entity.Keyword;
import sulhoe.aura.entity.User;
import sulhoe.aura.repository.KeywordRepository;
import sulhoe.aura.repository.UserRepository;
import sulhoe.aura.service.firebase.PushNotificationService;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 관리자 수동 발송용 API
 * - 토픽 발송
 * - 특정 사용자들 발송
 * - 전역 키워드 구독자들에게 발송
 *
 * 운영 권한은 app.admin.emails 로 간단히 화이트리스트 체크합니다.
 * (프로젝트에 ROLE 기반 권한이 있으면 @PreAuthorize 로 교체해도 됩니다)
 */
@Slf4j
@RestController
@RequestMapping("/admin/push")
@RequiredArgsConstructor
public class AdminPushController {

    private final PushNotificationService push;
    private final UserRepository userRepo;
    private final KeywordRepository keywordRepo;

    @Value("${app.admin.emails:}") // 쉼표 구분
    private String adminEmails;

    /* ===== 권한 체크 (화이트리스트) ===== */
    private String currentEmail() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth == null || !auth.isAuthenticated()) throw new RuntimeException("인증 필요");
        @SuppressWarnings("unchecked")
        Map<String, String> p = (Map<String, String>) auth.getPrincipal();
        String email = p.get("email");
        if (email == null) throw new RuntimeException("이메일 식별 실패");
        return email;
    }

    private void ensureAdmin() {
        String email = currentEmail();
        Set<String> allow = Arrays.stream(Optional.ofNullable(adminEmails).orElse("")
                        .split(","))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .collect(Collectors.toSet());
        if (!allow.contains(email)) {
            log.warn("[ADMIN-PUSH] 권한 없음: {}", email);
            throw new RuntimeException("관리자 권한이 필요합니다.");
        }
    }

    /* ===== 요청 DTO ===== */
    public record TopicReq(String topic, String type, String title, String link) {}
    public record UsersReq(List<Long> userIds, String type, String title, String link) {}
    public record KeywordsReq(List<Long> keywordIds, String type, String title, String link) {}

    /* ===== 1) 임의 토픽으로 발송 ===== */
    @PostMapping("/topic")
    public ResponseEntity<ApiResponse<Void>> sendToTopic(@RequestBody TopicReq req) {
        ensureAdmin();
        String topic = (req.topic() == null || req.topic().isBlank()) ? "notices" : req.topic().trim();
        push.sendToTopic(topic, req.type(), req.title(), req.link());
        return ResponseEntity.ok(ApiResponse.success(null));
    }

    /* ===== 2) 특정 사용자(IDs)에게 발송 ===== */
    @PostMapping("/users")
    public ResponseEntity<ApiResponse<Map<String, Object>>> sendToUsers(@RequestBody UsersReq req) {
        ensureAdmin();
        if (req.userIds() == null || req.userIds().isEmpty()) {
            return ResponseEntity.badRequest().body(ApiResponse.error(400, "userIds가 비어있습니다.", null));
        }
        int sent = 0;
        for (Long uid : new LinkedHashSet<>(req.userIds())) {
            if (uid == null) continue;
            Optional<User> u = userRepo.findById(uid);
            if (u.isPresent()) {
                push.sendToUserTopic(uid, req.type(), req.title(), req.link());
                sent++;
            }
        }
        return ResponseEntity.ok(ApiResponse.success(Map.of("requested", req.userIds().size(), "sent", sent)));
    }

    /* ===== 3) 전역 키워드 구독자들에게 발송 ===== */
    @PostMapping("/keywords")
    public ResponseEntity<ApiResponse<Map<String, Object>>> sendToKeywordSubscribers(@RequestBody KeywordsReq req) {
        ensureAdmin();
        if (req.keywordIds() == null || req.keywordIds().isEmpty()) {
            return ResponseEntity.badRequest().body(ApiResponse.error(400, "keywordIds가 비어있습니다.", null));
        }
        // 전역 키워드만 허용
        List<Keyword> globals = keywordRepo.findAllById(req.keywordIds()).stream()
                .filter(k -> k.getScope() == Keyword.Scope.GLOBAL)
                .toList();

        // 구독자 찾기 (User.subscribedKeywords 와 JPA가 만든 조인테이블 기반)
        Set<Long> targets = userRepo.findAll().stream()
                .filter(u -> u.getSubscribedKeywords().stream().anyMatch(globals::contains))
                .map(User::getId)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        int sent = 0;
        for (Long uid : targets) {
            push.sendToUserTopic(uid, req.type(), req.title(), req.link());
            sent++;
        }
        return ResponseEntity.ok(ApiResponse.success(Map.of(
                "keywords", globals.stream().map(Keyword::getId).toList(),
                "targets", targets.size(),
                "sent", sent
        )));
    }
}
