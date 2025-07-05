package sulhoe.ajouhub.repository;

import sulhoe.ajouhub.entity.Notice;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface NoticeRepository extends JpaRepository<Notice, UUID> {
    Optional<Notice> findByLink(String title);

}
