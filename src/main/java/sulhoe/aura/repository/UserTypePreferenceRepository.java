package sulhoe.aura.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import sulhoe.aura.entity.UserTypePreference;

import java.util.List;
import java.util.Optional;

public interface UserTypePreferenceRepository extends JpaRepository<UserTypePreference, Long> {
    Optional<UserTypePreference> findByUserIdAndType(Long userId, String type);

    @Query("select utp.user.id from UserTypePreference utp where utp.type = :type and utp.mode = 'ALL'")
    List<Long> findAllUserIdsByTypeAndAll(@Param("type") String type);

    @Query("select utp.user.id from UserTypePreference utp where utp.type = :type and utp.mode = 'KEYWORD'")
    List<Long> findAllUserIdsByTypeAndKeyword(@Param("type") String type);
}
