package sulhoe.aura.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;
import sulhoe.aura.dto.ApiResponse;
import sulhoe.aura.entity.UserTypePreference;
import sulhoe.aura.repository.UserRepository;
import sulhoe.aura.service.keyword.KeywordService;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/subscriptions/types")
@RequiredArgsConstructor
public class SubscriptionController {

    private final KeywordService keywordService;
    private final UserRepository userRepo;

    private Long currentUserId() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        @SuppressWarnings("unchecked")
        Map<String, String> p = (Map<String, String>) auth.getPrincipal();
        return userRepo.findByEmail(p.get("email")).orElseThrow().getId();
    }

    /** 현재 type 구독 모드 조회 */
    @GetMapping("/{type}")
    public ResponseEntity<ApiResponse<UserTypePreference.Mode>> getTypeMode(@PathVariable String type) {
        Long uid = currentUserId();
        return ResponseEntity.ok(ApiResponse.success(keywordService.getTypeMode(uid, type)));
    }

    /** 모드 설정: ALL | KEYWORD | NONE (value 쿼리 파라미터) */
    @PostMapping("/{type}/mode")
    public ResponseEntity<ApiResponse<Void>> setTypeMode(
            @PathVariable String type,
            @RequestParam("value") String modeValue
    ) {
        Long uid = currentUserId();
        UserTypePreference.Mode mode = UserTypePreference.Mode.valueOf(modeValue.toUpperCase());
        keywordService.setTypeMode(uid, type, mode);
        return ResponseEntity.ok(ApiResponse.success(null));
    }

    /** 해당 type에서 내가 구독한 키워드 ID 목록 */
    @GetMapping("/{type}/keywords")
    public ResponseEntity<ApiResponse<List<Long>>> listUserTypeKeywordIds(@PathVariable String type) {
        Long uid = currentUserId();
        return ResponseEntity.ok(ApiResponse.success(keywordService.listUserTypeKeywordIds(uid, type)));
    }

    /** 키워드 구독 추가 (전역/개인 자동 판별) */
    @PostMapping("/{type}/keywords")
    public ResponseEntity<ApiResponse<Long>> addUserTypeKeyword(
            @PathVariable String type,
            @RequestParam("keywordId") Long keywordId
    ) {
        Long uid = currentUserId();
        Long id = keywordService.addUserTypeKeyword(uid, type, keywordId);
        return ResponseEntity.ok(ApiResponse.success(id));
    }

    /** 키워드 구독 해제 */
    @DeleteMapping("/{type}/keywords/{keywordId}")
    public ResponseEntity<ApiResponse<Void>> removeUserTypeKeyword(
            @PathVariable String type,
            @PathVariable Long keywordId
    ) {
        Long uid = currentUserId();
        keywordService.removeUserTypeKeyword(uid, type, keywordId);
        return ResponseEntity.ok(ApiResponse.success(null));
    }
}
