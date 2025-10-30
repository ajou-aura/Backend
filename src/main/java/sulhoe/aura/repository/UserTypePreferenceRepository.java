package sulhoe.aura.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
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

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("delete from UserTypePreference utp where utp.user.id = :userId and utp.type = :type")
    int deleteByUserIdAndType(@Param("userId") Long userId, @Param("type") String type);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("delete from UserTypePreference utp where utp.user.id = :userId and utp.type in :types")
    int deleteAllByUserAndTypes(@Param("userId") Long userId, @Param("types") List<String> types);
}
