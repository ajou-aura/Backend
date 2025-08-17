package sulhoe.aura.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import sulhoe.aura.dto.ApiResponse;
import sulhoe.aura.dto.notice.NoticeDto;
import sulhoe.aura.dto.user.DepartmentRequestDto;
import sulhoe.aura.dto.user.UserResponseDto;
import sulhoe.aura.service.login.UserService;

import java.util.List;
import java.util.Set;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/user")
@RequiredArgsConstructor
public class UserController {
    private final UserService userService;

    @GetMapping("/info")
    public ResponseEntity<ApiResponse<UserResponseDto>> userInfo(@RequestHeader(value = "Authorization", required = false) String header) {
        log.debug("[USER-INFO] 호출됨. Authorization 헤더={}", header);

        if (header == null || !header.startsWith("Bearer ")) {
            log.warn("[USER-INFO] Bearer 토큰 누락 또는 형식 오류");
            throw new ResponseStatusException(org.springframework.http.HttpStatus.UNAUTHORIZED, "유효한 Bearer 토큰이 필요합니다.");
        }

        String token = header.substring(7);
        try {
            UserResponseDto dto = userService.getUserInfo(token);
            log.debug("[USER-INFO] 조회 성공: {}", dto.email());
            return ResponseEntity.ok(ApiResponse.success(dto));
        } catch (Exception e) {
            log.warn("[USER-INFO] 조회 실패: {}", e.getMessage());
            throw new ResponseStatusException(org.springframework.http.HttpStatus.UNAUTHORIZED, e.getMessage(), e);
        }
    }

    // 학과 목록 조회
    @GetMapping("/departments")
    public ResponseEntity<ApiResponse<Set<String>>> getDepartments(@RequestHeader("Authorization") String header) {
        String token = extractToken(header);
        return ResponseEntity.ok(ApiResponse.success(userService.getDepartments(token)));
    }

    // 학과 추가
    @PostMapping("/departments")
    public ResponseEntity<ApiResponse<Void>> addDepartment(@RequestHeader("Authorization") String header, @RequestBody DepartmentRequestDto dto) {
        String token = extractToken(header);
        userService.addDepartment(token, dto.department());
        return ResponseEntity.ok(ApiResponse.success(null));
    }

    // 학과 삭제
    @DeleteMapping("/departments")
    public ResponseEntity<ApiResponse<Void>> removeDepartment(@RequestHeader("Authorization") String header, @RequestBody DepartmentRequestDto dto) {
        String token = extractToken(header);
        userService.removeDepartment(token, dto.department());
        return ResponseEntity.ok(ApiResponse.success(null));
    }

    // 공지사항 북마크 추가
    @PostMapping("/saved-notices/{id}")
    public ResponseEntity<ApiResponse<Void>> saveNotice(@RequestHeader("Authorization") String header, @PathVariable("id") UUID noticeId) {

        String token = extractToken(header);
        userService.saveNotice(token, noticeId);
        return ResponseEntity.ok(ApiResponse.success(null));
    }

    // 공지사항 북마크 해제
    @DeleteMapping("/saved-notices/{id}")
    public ResponseEntity<ApiResponse<Void>> removeSavedNotice(@RequestHeader("Authorization") String header, @PathVariable("id") UUID noticeId) {

        String token = extractToken(header);
        userService.removeSavedNotice(token, noticeId);
        return ResponseEntity.ok(ApiResponse.success(null));
    }

    // 공지사항 북마크 목록 조회
    @GetMapping("/saved-notices")
    public ResponseEntity<ApiResponse<List<NoticeDto>>> getSavedNotices(@RequestHeader("Authorization") String header) {
        String token = extractToken(header);
        List<NoticeDto> list = userService.getSavedNotices(token);
        return ResponseEntity.ok(ApiResponse.success(list));
    }

    // 공통: 헤더 → 토큰 검사
    private String extractToken(String header) {
        if (header == null || !header.startsWith("Bearer ")) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "유효한 Bearer 토큰이 필요합니다.");
        }
        return header.substring(7);
    }
}
