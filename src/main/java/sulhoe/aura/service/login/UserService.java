package sulhoe.aura.service.login;

import io.jsonwebtoken.JwtException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import sulhoe.aura.config.JwtTokenProvider;
import sulhoe.aura.dto.notice.NoticeDto;
import sulhoe.aura.dto.user.UserResponseDto;
import sulhoe.aura.entity.Notice;
import sulhoe.aura.entity.User;
import sulhoe.aura.repository.NoticeRepository;
import sulhoe.aura.repository.UserRepository;

import java.util.List;
import java.util.Set;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserService {
    private final UserRepository userRepository;
    private final NoticeRepository noticeRepository;

    @Transactional(readOnly = true)
    public UserResponseDto getUserInfoByEmail(String email) {
        User user = findUserByEmail(email);
        log.debug("[SVC][USER-INFO] email={} name={}", user.getEmail(), user.getName());
        return new UserResponseDto(user.getName(), user.getEmail(), user.getDepartments());
    }

    @Transactional(readOnly = true)
    public Set<String> getDepartmentsByEmail(String email) {
        User user = findUserByEmail(email);
        log.debug("[SVC][DEPTS] 조회: email={}, count={}", email, user.getDepartments().size());
        return user.getDepartments();
    }

    @Transactional
    public void addDepartmentByEmail(String email, String dept) {
        User user = findUserByEmail(email);
        boolean added = user.getDepartments().add(dept);
        userRepository.save(user);
        log.info("[SVC][DEPTS] 추가: email={}, dept={}, added={}", email, dept, added);
    }

    @Transactional
    public void removeDepartmentByEmail(String email, String dept) {
        User user = findUserByEmail(email);
        boolean removed = user.getDepartments().remove(dept);
        userRepository.save(user);
        log.info("[SVC][DEPTS] 삭제: email={}, dept={}, removed={}", email, dept, removed);
    }

    @Transactional
    public void saveNoticeByEmail(String email, UUID noticeId) {
        User user = findUserByEmail(email);
        Notice notice = noticeRepository.findById(noticeId)
                .orElseThrow(() -> new IllegalArgumentException("공지사항을 찾을 수 없습니다: " + noticeId));
        boolean added = user.getSavedNotices().add(notice);
        userRepository.save(user);
        log.info("[SVC][BOOKMARK] 추가: email={}, noticeId={}, added={}", email, noticeId, added);
    }

    @Transactional
    public void removeSavedNoticeByEmail(String email, UUID noticeId) {
        User user = findUserByEmail(email);
        Notice notice = noticeRepository.findById(noticeId)
                .orElseThrow(() -> new IllegalArgumentException("공지사항을 찾을 수 없습니다: " + noticeId));
        boolean removed = user.getSavedNotices().remove(notice);
        userRepository.save(user);
        log.info("[SVC][BOOKMARK] 삭제: email={}, noticeId={}, removed={}", email, noticeId, removed);
    }

    @Transactional(readOnly = true)
    public List<NoticeDto> getSavedNoticesByEmail(String email) {
        User user = findUserByEmail(email);
        List<NoticeDto> list = user.getSavedNotices().stream().map(NoticeDto::fromEntity).toList();
        log.debug("[SVC][BOOKMARK] 조회: email={}, count={}", email, list.size());
        return list;
    }

    /** 공통: 이메일로 사용자 조회 + 로그 */
    private User findUserByEmail(String email) {
        return userRepository.findByEmail(email)
                .orElseThrow(() -> {
                    log.warn("[SVC][USER] 미존재: email={}", email);
                    return new IllegalArgumentException("사용자를 찾을 수 없습니다: " + email);
                });
    }
}
