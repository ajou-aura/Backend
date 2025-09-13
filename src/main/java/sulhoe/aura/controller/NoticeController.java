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
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        @SuppressWarnings("unchecked")
        Map<String, String> p = (Map<String, String>) auth.getPrincipal();
        return userRepo.findByEmail(p.get("email")).orElseThrow().getId();
    }

    /**
     * GET /notices/page
     *   ?type=학사
     *   &search=등록
     *   &globalIds=1,2
     *   &personalIds=10,11
     *   &page=0&size=10&sort=date,desc
     *
     * - 키워드가 전달되면 "키워드 매칭" (전역 OR 개인)
     * - search 는 항상 AND로 추가 필터 (없으면 무시)
     * - 키워드/검색 둘 다 없으면 전체/타입별 목록
     */
    @GetMapping("/page")
    public ResponseEntity<ApiResponse<Page<NoticeDto>>> getPagedNotices(
            @RequestParam(value = "type",   required = false) String type,
            @RequestParam(value = "search", required = false) String search,
            @RequestParam(value = "globalIds",  required = false) List<Long> globalIds,
            @RequestParam(value = "personalIds",required = false) List<Long> personalIds,
            @PageableDefault(size = 10, sort = "date", direction = Sort.Direction.DESC)
            Pageable pageable
    ) {
        // null → 빈 리스트로 보정
        globalIds   = (globalIds   == null) ? Collections.emptyList() : globalIds;
        personalIds = (personalIds == null) ? Collections.emptyList() : personalIds;

        Page<Notice> pages;
        boolean hasGlobal   = !globalIds.isEmpty();
        boolean hasPersonal = !personalIds.isEmpty();
        boolean hasKeyword  = hasGlobal || hasPersonal;

        if (hasKeyword) {
            Long uid = currentUserId();

            if (hasGlobal && hasPersonal) {
                pages = noticeRepository.findByGlobalOrPersonalKeywordIdsWithSearch(
                        uid, type, globalIds, personalIds, search, pageable
                );
            } else if (hasGlobal) {
                pages = noticeRepository.findByGlobalKeywordIdsWithSearch(
                        type, globalIds, search, pageable
                );
            } else { // personal only
                pages = noticeRepository.findByPersonalKeywordIdsWithSearch(
                        uid, type, personalIds, search, pageable
                );
            }
        } else if (search != null && !search.isBlank()) {
            // 자유 검색
            pages = (type != null)
                    ? noticeRepository.findByTypeAndTitleContainingIgnoreCase(type, search, pageable)
                    : noticeRepository.findByTitleContainingIgnoreCase(search, pageable);
        } else {
            // 전체/카테고리만
            pages = (type != null)
                    ? noticeRepository.findByType(type, pageable)
                    : noticeRepository.findAll(pageable);
        }

        return ResponseEntity.ok(ApiResponse.success(pages.map(NoticeDto::fromEntity)));
    }
}
