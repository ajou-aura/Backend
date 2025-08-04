package sulhoe.ajouhub.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import sulhoe.ajouhub.dto.ApiResponse;
import sulhoe.ajouhub.dto.notice.NoticeDto;
import sulhoe.ajouhub.entity.Notice;
import sulhoe.ajouhub.repository.NoticeRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

@RestController
@RequestMapping("/notices")
@RequiredArgsConstructor
public class NoticeController {

    private final NoticeRepository noticeRepository;
    /**
     * 전체/카테고리/검색 + 페이징을 한 번에 처리
     *
     *   GET /notices/page?type=""search=type&page=0&size=5&sort=date,desc
     *   [&type=학사]
     *   [&search=등록]
     */
    @GetMapping("/page")
    public ResponseEntity<ApiResponse<Page<NoticeDto>>> getPagedNotices(
            @RequestParam(value = "type",   required = false) String type,
            @RequestParam(value = "search", required = false) String search,
            @PageableDefault(size = 10, sort = "date", direction = Sort.Direction.DESC)
            Pageable pageable
    ) {
        Page<Notice> pages;
        try {
            if (type != null && search != null) {
                pages = noticeRepository
                        .findByTypeAndTitleContainingIgnoreCase(type, search, pageable);
            }
            else if (type != null) {
                pages = noticeRepository.findByType(type, pageable);
            }
            else if (search != null) {
                pages = noticeRepository.findByTitleContainingIgnoreCase(search, pageable);
            }
            else {
                pages = noticeRepository.findAll(pageable);
            }

            Page<NoticeDto> dtoPage = pages.map(NoticeDto::fromEntity);
            return ResponseEntity.ok(ApiResponse.success(dtoPage));
        } catch(Exception e) {
            return ResponseEntity.badRequest().body(ApiResponse.error(400, "잘못된 요청 파라미터입니다.", null));
        }
    }
}
