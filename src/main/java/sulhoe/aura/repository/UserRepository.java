package sulhoe.aura.repository;

import sulhoe.aura.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface UserRepository extends JpaRepository<User, Long> {
    Optional<User> findByEmail(String email);
    /** 전역 키워드 구독자 조회(FCM 타겟 계산용) */
    List<User> findAllBySubscribedKeywords_IdIn(List<Long> keywordIds);
}
