package sulhoe.aura.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import sulhoe.aura.dto.ApiResponse;
import sulhoe.aura.dto.notice.NoticeDto;
import sulhoe.aura.dto.user.DepartmentRequestDto;
import sulhoe.aura.dto.user.UserResponseDto;
import sulhoe.aura.service.login.UserService;
import org.springframework.security.core.Authentication;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/user")
@RequiredArgsConstructor
public class UserController {
    private final UserService userService;

    // JwtAuthenticationFilter가 principal(Map<email,name>)을 넣어줌
    private String currentEmail() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth == null || !auth.isAuthenticated()) {
            log.warn("[AUTH] 인증 정보 없음 또는 비인증 상태");
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "인증이 필요합니다.");
        }
        @SuppressWarnings("unchecked")
        Map<String, String> p = (Map<String, String>) auth.getPrincipal();
        String email = p.get("email");
        if (email == null) {
            log.warn("[AUTH] principal에 email 없음: principal={}", p);
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "사용자 식별에 실패했습니다.");
        }
        return email;
    }

    @GetMapping("/info")
    public ResponseEntity<ApiResponse<UserResponseDto>> userInfo() {
        String email = currentEmail();
        log.info("[USER-INFO] 호출: email={}", email);
        UserResponseDto dto = userService.getUserInfoByEmail(email);
        log.info("[USER-INFO] 조회 성공: {}", dto.email());
        return ResponseEntity.ok(ApiResponse.success(dto));
    }

    // 학과 목록 조회
    @GetMapping("/departments")
    public ResponseEntity<ApiResponse<Set<String>>> getDepartments() {
        String email = currentEmail();
        log.info("[DEPTS] 목록 조회: email={}", email);
        return ResponseEntity.ok(ApiResponse.success(userService.getDepartmentsByEmail(email)));
    }

    // 학과 추가
    @PostMapping("/departments")
    public ResponseEntity<ApiResponse<Void>> addDepartment(@RequestBody DepartmentRequestDto dto) {
        String email = currentEmail();
        log.info("[DEPTS] 추가 요청: email={}, dept={}", email, dto.department());
        userService.addDepartmentByEmail(email, dto.department());
        return ResponseEntity.ok(ApiResponse.success(null));
    }

    // 학과 삭제
    @DeleteMapping("/departments")
    public ResponseEntity<ApiResponse<Void>> removeDepartment(@RequestBody DepartmentRequestDto dto) {
        String email = currentEmail();
        log.info("[DEPTS] 삭제 요청: email={}, dept={}", email, dto.department());
        userService.removeDepartmentByEmail(email, dto.department());
        return ResponseEntity.ok(ApiResponse.success(null));
    }

    // 공지사항 북마크 추가
    @PostMapping("/saved-notices/{id}")
    public ResponseEntity<ApiResponse<Void>> saveNotice(@PathVariable("id") UUID noticeId) {
        String email = currentEmail();
        log.info("[BOOKMARK] 추가 요청: email={}, noticeId={}", email, noticeId);
        userService.saveNoticeByEmail(email, noticeId);
        return ResponseEntity.ok(ApiResponse.success(null));
    }

    // 공지사항 북마크 해제
    @DeleteMapping("/saved-notices/{id}")
    public ResponseEntity<ApiResponse<Void>> removeSavedNotice(@PathVariable("id") UUID noticeId) {
        String email = currentEmail();
        log.info("[BOOKMARK] 삭제 요청: email={}, noticeId={}", email, noticeId);
        userService.removeSavedNoticeByEmail(email, noticeId);
        return ResponseEntity.ok(ApiResponse.success(null));
    }

    // 공지사항 북마크 목록 조회
    @GetMapping("/saved-notices")
    public ResponseEntity<ApiResponse<List<NoticeDto>>> getSavedNotices() {
        String email = currentEmail();
        log.info("[BOOKMARK] 목록 조회: email={}", email);
        List<NoticeDto> list = userService.getSavedNoticesByEmail(email);
        log.info("[BOOKMARK] 조회 결과: {}건", list.size());
        return ResponseEntity.ok(ApiResponse.success(list));
    }
}
