package sulhoe.aura.service.notice;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import sulhoe.aura.entity.Notice;
import sulhoe.aura.service.keyword.KeywordService;

import java.util.List;

@Service
@RequiredArgsConstructor
public class NoticeFanoutService {
    private final KeywordService keywordService;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void sendNotifications(List<Notice> notices, String type) {
        for (Notice n : notices) {
            try {
                keywordService.onNoticeSaved(n, type);
            } catch (Exception ex) {
                org.slf4j.LoggerFactory.getLogger(NoticeFanoutService.class)
                        .error("[FANOUT] notify failed for link={}: {}", n.getLink(), ex.toString());
            }
        }
    }
}
