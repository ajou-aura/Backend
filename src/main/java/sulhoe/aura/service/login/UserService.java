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
    private final JwtTokenProvider jwt;
    private final UserRepository userRepository;
    private final NoticeRepository noticeRepository;

    public UserResponseDto getUserInfo(String token) {
        // 1) 토큰 검사 및 사용자 조회
        User user = findUserByToken(token);

        // 2) DTO로 매핑하여 반환
        return new UserResponseDto(user.getName(), user.getEmail(), user.getDepartments());
    }

    @Transactional(readOnly = true)
    public Set<String> getDepartments(String token) {
        return findUserByToken(token).getDepartments();
    }

    @Transactional
    public void addDepartment(String token, String dept) {
        User user = findUserByToken(token);
        user.getDepartments().add(dept);
        userRepository.save(user);
    }

    @Transactional
    public void removeDepartment(String token, String dept) {
        User user = findUserByToken(token);
        user.getDepartments().remove(dept);
        userRepository.save(user);
    }

    @Transactional
    public void saveNotice(String token, UUID noticeId) {
        User user = findUserByToken(token);
        Notice notice = noticeRepository.findById(noticeId).orElseThrow(() -> new IllegalArgumentException("공지사항을 찾을 수 없습니다: " + noticeId));
        user.getSavedNotices().add(notice);
        userRepository.save(user);
    }

    @Transactional
    public void removeSavedNotice(String token, UUID noticeId) {
        User user = findUserByToken(token);
        Notice notice = noticeRepository.findById(noticeId).orElseThrow(() -> new IllegalArgumentException("공지사항을 찾을 수 없습니다: " + noticeId));
        user.getSavedNotices().remove(notice);
        userRepository.save(user);
    }

    @Transactional(readOnly = true)
    public List<NoticeDto> getSavedNotices(String token) {
        User user = findUserByToken(token);
        return user.getSavedNotices().stream().map(NoticeDto::fromEntity).toList();
    }

    // 공통: 토큰 → User 조회
    private User findUserByToken(String token) {
        if (!jwt.validateToken(token)) {
            throw new JwtException("Invalid token.");
        }
        String email = jwt.getEmail(token);
        return userRepository.findByEmail(email).orElseThrow(() -> new IllegalArgumentException("사용자를 찾을 수 없습니다: " + email));
    }
}
