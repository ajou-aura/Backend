package sulhoe.aura.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import sulhoe.aura.dto.ApiResponse;
import sulhoe.aura.dto.notice.NoticeDto;
import sulhoe.aura.entity.Notice;
import sulhoe.aura.repository.NoticeRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import sulhoe.aura.repository.UserRepository;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/notices")
@RequiredArgsConstructor
public class NoticeController {

    private final NoticeRepository noticeRepository;
    private final UserRepository userRepo;

    // 현재 유저 ID (KeywordController와 동일한 방식)
    private Long currentUserId() {
        String email = getEmail();
        if (email == null) {
            throw new org.springframework.web.server.ResponseStatusException(
                    org.springframework.http.HttpStatus.UNAUTHORIZED, "사용자 식별에 실패했습니다.");
        }
        return userRepo.findByEmailIgnoreCase(email.trim())
                .map(sulhoe.aura.entity.User::getId)
                .orElseThrow(() -> new org.springframework.web.server.ResponseStatusException(
                        org.springframework.http.HttpStatus.UNAUTHORIZED, "등록되지 않은 사용자입니다."));
    }

    private static String getEmail() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth == null || !auth.isAuthenticated()) {
            throw new org.springframework.web.server.ResponseStatusException(
                    org.springframework.http.HttpStatus.UNAUTHORIZED, "인증이 필요합니다.");
        }
        Object principal = auth.getPrincipal();
        if (!(principal instanceof Map)) {
            throw new org.springframework.web.server.ResponseStatusException(
                    org.springframework.http.HttpStatus.UNAUTHORIZED, "부적절한 인증 주체입니다.");
        }
        @SuppressWarnings("unchecked")
        Map<String, String> p = (Map<String, String>) principal;
        String email = p.get("email");
        return email;
    }

    /**
     * GET /notices/page
     *   ?type=학사
     *   &search=등록
     *   &globalIds=1,2
     *   &personalIds=10,11
     *   &match=any|all   // 추가: 키워드 매칭 방식 (기본 any)
     *   &page=0&size=10&sort=date,desc
     *
     * 규칙:
     * - search 가 주어지면 "검색 모드"로 동작하고, 키워드 파라미터는 **무시**.
     * - search 가 없고 globalIds/personalIds 중 하나라도 있으면 "키워드 매칭 모드".
     *   - match=any(기본): 전달된 키워드 중 하나라도 매칭되면 포함(OR).
     *   - match=all       : 전달된 키워드가 **전부** 매칭되어야 포함(AND).
     * - 둘 다 없으면 기본 목록(전체 또는 type 필터만).
     */
    @GetMapping("/page")
    public ResponseEntity<ApiResponse<Page<NoticeDto>>> getPagedNotices(
            @RequestParam(value = "type",   required = false) String type,
            @RequestParam(value = "search", required = false) String search,
            @RequestParam(value = "globalIds",   required = false) List<Long> globalIds,
            @RequestParam(value = "personalIds", required = false) List<Long> personalIds,
            @RequestParam(value = "match",  required = false, defaultValue = "any") String match,
            @PageableDefault(size = 10, sort = {"date", "createdAt"}, direction = Sort.Direction.DESC)
            Pageable pageable
    ) {
        // null → 빈 리스트로 보정
        globalIds   = (globalIds   == null) ? Collections.emptyList() : globalIds;
        personalIds = (personalIds == null) ? Collections.emptyList() : personalIds;

        // 1) 검색 모드: search가 있으면 키워드 파라미터는 무시
        if (search != null && !search.isBlank()) {
            Page<Notice> pages = (type != null)
                    ? noticeRepository.findByTypeAndTitleContainingIgnoreCase(type, search, pageable)
                    : noticeRepository.findByTitleContainingIgnoreCase(search, pageable);
            return ResponseEntity.ok(ApiResponse.success(pages.map(NoticeDto::fromEntity)));
        }

        // 2) 키워드 매칭 모드
        boolean hasGlobal   = !globalIds.isEmpty();
        boolean hasPersonal = !personalIds.isEmpty();
        boolean hasKeyword  = hasGlobal || hasPersonal;

        Page<Notice> pages;
        if (hasKeyword) {
            boolean all = "all".equalsIgnoreCase(match);
            Long uid = currentUserId();

            if (all) {
                // AND 매칭
                if (hasGlobal && hasPersonal) {
                    pages = noticeRepository.findByGlobalAndPersonalKeywordIdsAll(
                            uid, type, globalIds, globalIds.size(), personalIds, personalIds.size(), pageable
                    );
                } else if (hasGlobal) {
                    pages = noticeRepository.findByGlobalKeywordIdsAll(
                            type, globalIds, globalIds.size(), pageable
                    );
                } else { // personal only
                    pages = noticeRepository.findByPersonalKeywordIdsAll(
                            uid, type, personalIds, personalIds.size(), pageable
                    );
                }
            } else {
                // ANY 매칭 (OR)
                if (hasGlobal && hasPersonal) {
                    pages = noticeRepository.findByGlobalOrPersonalKeywordIdsAny(
                            uid, type, globalIds, personalIds, pageable
                    );
                } else if (hasGlobal) {
                    pages = noticeRepository.findByGlobalKeywordIdsAny(
                            type, globalIds, pageable
                    );
                } else { // personal only
                    pages = noticeRepository.findByPersonalKeywordIdsAny(
                            uid, type, personalIds, pageable
                    );
                }
            }
        } else {
            // 3) 기본 목록
            pages = (type != null)
                    ? noticeRepository.findByType(type, pageable)
                    : noticeRepository.findAll(pageable);
        }

        return ResponseEntity.ok(ApiResponse.success(pages.map(NoticeDto::fromEntity)));
    }
}
