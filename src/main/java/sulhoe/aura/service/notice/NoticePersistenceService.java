package sulhoe.aura.service.notice;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import sulhoe.aura.entity.Notice;
import sulhoe.aura.repository.NoticeRepository;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class NoticePersistenceService {
    private final NoticeRepository noticeRepo;

    /** 청크 단위 진입: 내부에서 항목별 REQUIRES_NEW로 처리하여 fail-soft */
    @Transactional(propagation = Propagation.SUPPORTS, readOnly = false)
    public List<Notice> persistNotices(List<Notice> scraped) {
        if (scraped == null || scraped.isEmpty()) {
            return List.of();
        }

        List<String> links = scraped.stream()
                .filter(Objects::nonNull)
                .map(Notice::getLink)
                .filter(Objects::nonNull)
                .distinct()
                .toList();

        Map<String, Notice> existingByLink = links.isEmpty()
                ? new LinkedHashMap<>()
                : noticeRepo.findByLinkIn(links).stream()
                .filter(existing -> existing.getLink() != null)
                .collect(Collectors.toMap(
                        Notice::getLink,
                        existing -> existing,
                        (left, right) -> left,
                        LinkedHashMap::new
                ));

        Map<String, Notice> newByLink = new LinkedHashMap<>();
        LinkedHashSet<String> updatedExistingLinks = new LinkedHashSet<>();

        for (Notice n : scraped) {
            if (n == null) {
                continue;
            }

            Notice existing = existingByLink.get(n.getLink());
            if (existing != null) {
                if (isUpdated(existing, n)) {
                    merge(existing, n);
                    updatedExistingLinks.add(n.getLink());
                }
                continue;
            }

            Notice pendingInsert = newByLink.get(n.getLink());
            if (pendingInsert == null) {
                newByLink.put(n.getLink(), n);
                continue;
            }

            if (isUpdated(pendingInsert, n)) {
                merge(pendingInsert, n);
            }
        }

        List<Notice> newOrUpdated = new ArrayList<>();
        if (!newByLink.isEmpty()) {
            newOrUpdated.addAll(noticeRepo.saveAll(new ArrayList<>(newByLink.values())));
        }

        if (!updatedExistingLinks.isEmpty()) {
            List<Notice> updates = updatedExistingLinks.stream()
                    .map(existingByLink::get)
                    .toList();
            newOrUpdated.addAll(noticeRepo.saveAll(updates));
        }

        return newOrUpdated;
    }

    /** 항목별 신규/업데이트 저장 – 새로운 트랜잭션 경계 */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Notice saveOrUpdateOne(Notice n) {
        Optional<Notice> existingOpt = noticeRepo.findByLink(n.getLink());
        if (existingOpt.isEmpty()) {
            return noticeRepo.save(n);
        } else {
            Notice existing = existingOpt.get();
            if (isUpdated(existing, n)) {
                merge(existing, n);
                return noticeRepo.save(existing);
            }
        }
        return null; // 업데이트 필요 없음
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
