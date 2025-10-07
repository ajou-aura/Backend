package sulhoe.aura.repository;

import sulhoe.aura.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

public interface UserRepository extends JpaRepository<User, Long> {
    Optional<User> findByEmail(String email);
    Optional<User> findByEmailIgnoreCase(String email);
    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update User u set u.refreshToken = :newRt where u.email = :email and u.refreshToken = :oldRt")
    int rotateRefreshTokenAtomically(String email, String oldRt, String newRt);

    boolean existsByEmail(String email);
}
