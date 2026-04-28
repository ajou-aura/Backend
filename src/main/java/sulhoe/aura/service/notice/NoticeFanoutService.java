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
        org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(NoticeFanoutService.class);
        log.info("[FANOUT] Starting notification fanout for type={}, notices={}", type, notices.size());
        
        keywordService.refreshGlobalCache(); // refresh once per fanout cycle
        for (Notice n : notices) {
            try {
                log.info("[FANOUT] Processing notice: id={}, title={}", n.getId(), n.getTitle());
                keywordService.onNoticeSaved(n, type);
                log.info("[FANOUT] Successfully processed notice: id={}", n.getId());
            } catch (Exception ex) {
                log.error("[FANOUT] notify failed for link={}: {}", n.getLink(), ex.toString(), ex);
            }
        }
        log.info("[FANOUT] Completed notification fanout for type={}", type);
    }
}
