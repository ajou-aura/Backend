package sulhoe.aura.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import sulhoe.aura.entity.Notice;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface NoticeRepository extends JpaRepository<Notice, UUID> {
    Optional<Notice> findByLink(String link);
    boolean existsByType(String type);

    // 기본 페이징
    Page<Notice> findByType(String type, Pageable pageable);
    boolean existsByLink(String link);
    Page<Notice> findByTitleContainingIgnoreCase(String title, Pageable pageable);
    Page<Notice> findByTypeAndTitleContainingIgnoreCase(String type, String title, Pageable pageable);

    @Query("select n from Notice n left join fetch n.keywords where n.id = :id")
    Optional<Notice> findByIdWithKeywords(@Param("id") UUID id);
    long countByTypeAndCreatedAtAfter(String type, LocalDateTime dateTime);

    /* ========== 키워드 매칭: ANY (OR) ========== */

    // 전역 키워드 ANY
    @Query("""
        select distinct n
        from Notice n
          join n.keywords gk
        where (:type is null or n.type = :type)
          and gk.id in :globalIds
        """)
    Page<Notice> findByGlobalKeywordIdsAny(
            @Param("type") String type,
            @Param("globalIds") List<Long> globalIds,
            Pageable pageable
    );

    // 개인 키워드 ANY (현재 사용자 소유 + 제목 포함)
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
        """)
    Page<Notice> findByPersonalKeywordIdsAny(
            @Param("userId") Long userId,
            @Param("type") String type,
            @Param("personalIds") List<Long> personalIds,
            Pageable pageable
    );

    // 전역 OR 개인 ANY 혼합
    @Query("""
        select distinct n
        from Notice n
          left join n.keywords gk
        where (:type is null or n.type = :type)
          and (
               ( :globalIds is not null and gk.id in :globalIds )
            or exists (
                 select 1 from Keyword k
                 where k.id in :personalIds
                   and k.ownerId = :userId
                   and lower(n.title) like concat('%', lower(k.phrase), '%')
            )
          )
        """)
    Page<Notice> findByGlobalOrPersonalKeywordIdsAny(
            @Param("userId") Long userId,
            @Param("type") String type,
            @Param("globalIds") List<Long> globalIds,
            @Param("personalIds") List<Long> personalIds,
            Pageable pageable
    );

    /* ========== 키워드 매칭: ALL (AND) ========== */
    // 전역 키워드 ALL: 모든 globalIds가 notice_keywords 에 있어야 함
    @Query("""
        select n
        from Notice n
        where (:type is null or n.type = :type)
          and ( :globalCount = 0
                or n.id in (
                    select n2.id
                    from Notice n2
                      join n2.keywords g2
                    where g2.id in :globalIds
                    group by n2.id
                    having count(distinct g2.id) = :globalCount
                )
              )
        """)
    Page<Notice> findByGlobalKeywordIdsAll(
            @Param("type") String type,
            @Param("globalIds") List<Long> globalIds,
            @Param("globalCount") int globalCount,
            Pageable pageable
    );

    // 개인 키워드 ALL: 제목이 내 personalIds 모든 phrase 를 포함해야 함
    @Query("""
        select n
        from Notice n
        where (:type is null or n.type = :type)
          and ( :personalCount = 0
                or n.id in (
                    select n3.id
                    from Notice n3, Keyword k
                    where k.id in :personalIds
                      and k.ownerId = :userId
                      and lower(n3.title) like concat('%', lower(k.phrase), '%')
                    group by n3.id
                    having count(distinct k.id) = :personalCount
                )
              )
        """)
    Page<Notice> findByPersonalKeywordIdsAll(
            @Param("userId") Long userId,
            @Param("type") String type,
            @Param("personalIds") List<Long> personalIds,
            @Param("personalCount") int personalCount,
            Pageable pageable
    );

    // 전역 + 개인 ALL: 두 조건을 모두 만족해야 함
    @Query("""
        select n
        from Notice n
        where (:type is null or n.type = :type)
          and (
                :globalCount = 0 or n.id in (
                    select n2.id
                    from Notice n2
                      join n2.keywords g2
                    where g2.id in :globalIds
                    group by n2.id
                    having count(distinct g2.id) = :globalCount
                )
              )
          and (
                :personalCount = 0 or n.id in (
                    select n3.id
                    from Notice n3, Keyword k
                    where k.id in :personalIds
                      and k.ownerId = :userId
                      and lower(n3.title) like concat('%', lower(k.phrase), '%')
                    group by n3.id
                    having count(distinct k.id) = :personalCount
                )
              )
        """)
    Page<Notice> findByGlobalAndPersonalKeywordIdsAll(
            @Param("userId") Long userId,
            @Param("type") String type,
            @Param("globalIds") List<Long> globalIds,
            @Param("globalCount") int globalCount,
            @Param("personalIds") List<Long> personalIds,
            @Param("personalCount") int personalCount,
            Pageable pageable
    );
}
