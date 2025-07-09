package sulhoe.ajouhub.service.notice;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import sulhoe.ajouhub.entity.Notice;
import sulhoe.ajouhub.repository.NoticeRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class NoticePersistenceService {
    private final NoticeRepository noticeRepo;

    // scraped 중에 신규 또는 업데이트된 엔티티만 저장/업데이트하고 그 목록을 반환
    @Transactional
    public List<Notice> persistNotices(List<Notice> scraped) {
        List<Notice> newOrUpdated = new ArrayList<>();
        for (Notice n : scraped) {
            Optional<Notice> existingOpt = noticeRepo.findByLink(n.getLink());
            if (existingOpt.isEmpty()) {
                noticeRepo.save(n);
                newOrUpdated.add(n);
            } else {
                Notice existing = existingOpt.get();
                if (isUpdated(existing, n)) {
                    merge(existing, n);
                    noticeRepo.save(existing);
                    newOrUpdated.add(existing);
                }
            }
        }
        return newOrUpdated;
    }

    private boolean isUpdated(Notice oldOne, Notice newOne) {
        return !Objects.equals(oldOne.getType(), newOne.getType())
                || !Objects.equals(oldOne.getTitle(), newOne.getTitle())
                || !Objects.equals(oldOne.getDate(), newOne.getDate())
                || !Objects.equals(oldOne.getDepartment(), newOne.getDepartment())
                || !Objects.equals(oldOne.getCategory(), newOne.getCategory());
    }

    private void merge(Notice target, Notice src) {
        target.setType(src.getType());
        target.setTitle(src.getTitle());
        target.setDate(src.getDate());
        target.setDepartment(src.getDepartment());
        target.setCategory(src.getCategory());
        target.setNumber(src.getNumber());
    }
}
