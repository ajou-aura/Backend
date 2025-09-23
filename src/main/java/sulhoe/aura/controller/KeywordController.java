package sulhoe.aura.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;
import sulhoe.aura.dto.ApiResponse;
import sulhoe.aura.entity.Keyword;
import sulhoe.aura.repository.UserRepository;
import sulhoe.aura.service.keyword.KeywordService;

import java.net.URI;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/keywords")
@RequiredArgsConstructor
public class KeywordController {

    private final KeywordService keywordService;
    private final UserRepository userRepo;

    // JwtAuthenticationFilter가 principal(Map<email,name>)을 넣어줌 → userId 조회 필요시 email로 찾음
    private Long currentUserId() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth == null || !auth.isAuthenticated()) {
            throw new IllegalStateException("인증이 필요합니다.");
        }
        @SuppressWarnings("unchecked")
        Map<String, String> p = (Map<String, String>) auth.getPrincipal();
        String email = p.get("email");
        return userRepo.findByEmail(email).orElseThrow().getId();
    }

    // 전체 키워드 목록 (전역 + 내 개인)
    @GetMapping
    public ResponseEntity<ApiResponse<List<Keyword>>> list() {
        Long uid = currentUserId();
        return ResponseEntity.ok(ApiResponse.success(keywordService.listAllForUser(uid)));
    }

    /** 내 개인 키워드 추가 */
    @PostMapping
    public ResponseEntity<ApiResponse<Keyword>> create(@RequestParam String phrase) {
        Long uid = currentUserId();
        Keyword k = keywordService.addMyKeyword(uid, phrase);
        return ResponseEntity.created(URI.create("/keywords/" + k.getId())).body(ApiResponse.success(k));
    }

    // 내 개인 키워드 삭제
    @DeleteMapping("/{id}")
    public ResponseEntity<ApiResponse<Void>> delete(@PathVariable Long id) {
        Long uid = currentUserId();
        keywordService.deleteMyKeyword(uid, id);
        return ResponseEntity.ok(ApiResponse.success(null));
    }

    // 전역 키워드 구독
    @PostMapping("/subscribe/{globalKeywordId}")
    public ResponseEntity<ApiResponse<Void>> subscribe(@PathVariable Long globalKeywordId) {
        Long uid = currentUserId();
        keywordService.subscribeGlobal(uid, globalKeywordId);
        return ResponseEntity.created(URI.create("/keywords/subscribe/" + globalKeywordId))
                .body(ApiResponse.success(null));
    }

    // 전역 키워드 구독 해지
    @DeleteMapping("/subscribe/{globalKeywordId}")
    public ResponseEntity<ApiResponse<Void>> unsubscribe(@PathVariable Long globalKeywordId) {
        Long uid = currentUserId();
        keywordService.unsubscribeGlobal(uid, globalKeywordId);
        return ResponseEntity.ok(ApiResponse.success(null));
    }

    // 내 전역 구독 목록 ID
    @GetMapping("/subscriptions")
    public ResponseEntity<ApiResponse<List<Long>>> mySubs() {
        Long uid = currentUserId();
        return ResponseEntity.ok(ApiResponse.success(keywordService.myGlobalSubscriptionIds(uid)));
    }

    // 개인 키워드 구독
    @PostMapping("/subscribe/personal/{personalKeywordId}")
    public ResponseEntity<ApiResponse<Void>> subscribePersonal(@PathVariable Long personalKeywordId) {
        Long uid = currentUserId();
        keywordService.subscribePersonal(uid, personalKeywordId);
        return ResponseEntity
                .created(URI.create("/keywords/subscribe/personal/" + personalKeywordId))
                .body(ApiResponse.success(null));
    }

    /// 개인 키워드 구독 해지
    @DeleteMapping("/subscribe/personal/{personalKeywordId}")
    public ResponseEntity<ApiResponse<Void>> unsubscribePersonal(@PathVariable Long personalKeywordId) {
        Long uid = currentUserId();
        keywordService.unsubscribePersonal(uid, personalKeywordId);
        return ResponseEntity.ok(ApiResponse.success(null));
    }

    // 내 개인 키워드 구독 목록 ID
    @GetMapping("/subscriptions/personal")
    public ResponseEntity<ApiResponse<List<Long>>> myPersonalSubs() {
        Long uid = currentUserId();
        return ResponseEntity.ok(ApiResponse.success(keywordService.myPersonalSubscriptionIds(uid)));
    }
}
