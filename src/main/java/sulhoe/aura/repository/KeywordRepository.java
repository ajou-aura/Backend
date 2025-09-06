package sulhoe.aura.repository;

import org.springframework.data.jpa.repository.JpaRepository;
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
}
