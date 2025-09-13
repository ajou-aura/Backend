package sulhoe.aura.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import sulhoe.aura.entity.Notice;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface NoticeRepository extends JpaRepository<Notice, UUID> {
    Optional<Notice> findByLink(String link);
    boolean existsByType(String type);

    // 카테고리 페이징
    Page<Notice> findByType(String type, Pageable pageable);

    // 제목 검색  페이징 (대소문자 무시, 부분일치)
    Page<Notice> findByTitleContainingIgnoreCase(String title, Pageable pageable);

    // 카테고리 + 제목 검색  페이징
    Page<Notice> findByTypeAndTitleContainingIgnoreCase(String type, String title, Pageable pageable);

    @Query("select n from Notice n left join fetch n.keywords where n.id = :id")
    Optional<Notice> findByIdWithKeywords(@Param("id") UUID id);

    /* ── 키워드 매칭 전용(전역/개인/혼합 + search AND) ───────────────────────── */

    // 전역 키워드만: notice_keywords 조인
    @Query("""
        select distinct n
        from Notice n
          join n.keywords gk
        where (:type is null or n.type = :type)
          and gk.id in :globalIds
          and (:search is null or lower(n.title) like concat('%', lower(:search), '%'))
        """)
    Page<Notice> findByGlobalKeywordIdsWithSearch(
            @Param("type") String type,
            @Param("globalIds") List<Long> globalIds,
            @Param("search") String search,
            Pageable pageable
    );

    // 개인 키워드만: 제목 LIKE + 소유자 검증
    @Query("""
        select distinct n
        from Notice n
        where (:type is null or n.type = :type)
          and exists (
              select 1 from Keyword k
              where k.id in :personalIds
                and k.ownerId = :userId
                and lower(n.title) like concat('%', lower(k.phrase), '%')
          )
          and (:search is null or lower(n.title) like concat('%', lower(:search), '%'))
        """)
    Page<Notice> findByPersonalKeywordIdsWithSearch(
            @Param("userId") Long userId,
            @Param("type") String type,
            @Param("personalIds") List<Long> personalIds,
            @Param("search") String search,
            Pageable pageable
    );

    // 전역 OR 개인(둘 다 전달된 경우): OR 결합 + search AND
    @Query("""
        select distinct n
        from Notice n
          left join n.keywords gk
        where (:type is null or n.type = :type)
          and (
               gk.id in :globalIds
            or exists (
                 select 1 from Keyword k
                 where k.id in :personalIds
                   and k.ownerId = :userId
                   and lower(n.title) like concat('%', lower(k.phrase), '%')
            )
          )
          and (:search is null or lower(n.title) like concat('%', lower(:search), '%'))
        """)
    Page<Notice> findByGlobalOrPersonalKeywordIdsWithSearch(
            @Param("userId") Long userId,
            @Param("type") String type,
            @Param("globalIds") List<Long> globalIds,
            @Param("personalIds") List<Long> personalIds,
            @Param("search") String search,
            Pageable pageable
    );
}
