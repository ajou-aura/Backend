package sulhoe.aura.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import sulhoe.aura.entity.Keyword;
import sulhoe.aura.entity.Keyword.Scope;

import java.util.List;
import java.util.Optional;



public interface KeywordRepository extends JpaRepository<Keyword, Long> {
    boolean existsByScopeAndPhraseIgnoreCase(Scope scope, String phrase);
    boolean existsByOwnerIdAndPhraseIgnoreCase(Long ownerId, String phrase);
    List<Keyword> findAllByScope(Scope scope);
    List<Keyword> findAllByOwnerId(Long ownerId);
    Optional<Keyword> findByIdAndOwnerId(Long id, Long ownerId);

    // 제목에 걸리는 개인 키워드 소유자들만 DB에서 바로 가져오기
    @Query("""
        select distinct k.ownerId
        from Keyword k
        where k.scope = 'USER'
          and k.ownerId is not null
          and lower(:title) like concat('%', lower(k.phrase), '%')
    """)
    List<Long> findOwnerIdsMatchedByTitle(@Param("title") String title);
}
