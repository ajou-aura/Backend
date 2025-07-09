package sulhoe.ajouhub.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import sulhoe.ajouhub.entity.Notice;

import java.util.Optional;
import java.util.UUID;

public interface NoticeRepository extends JpaRepository<Notice, UUID> {
    Optional<Notice> findByLink(String link);

    // 카테고리 페이징
    Page<Notice> findByCategory(String category, Pageable pageable);

    // 제목 검색  페이징 (대소문자 무시, 부분일치)
    Page<Notice> findByTitleContainingIgnoreCase(String title, Pageable pageable);

    // 카테고리 + 제목 검색  페이징
    Page<Notice> findByCategoryAndTitleContainingIgnoreCase(String category, String title, Pageable pageable);

    boolean existsByType(String category);
}
