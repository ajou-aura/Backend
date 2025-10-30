package sulhoe.aura.service.notice;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import sulhoe.aura.config.NoticeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ScrapeScheduleService {
    private static final Logger log = LoggerFactory.getLogger(ScrapeScheduleService.class);
    private final NoticeConfig noticeConfig;
    private final NoticeScrapeService noticeScrapeService;

    // 시작 후 5초 뒤 한 번, 그 후 5분마다 실행
    @Scheduled(initialDelay = 5_000, fixedDelay = 300_000)
    public void runNoticeScraping() {
        List<String> failedTypes = new ArrayList<>();

        noticeConfig.getUrls().forEach((type, url) -> {
            try {
                noticeScrapeService.scrapeNotices(url, type);
                log.debug("Scraping success for type: {}", type);
            } catch (Exception e) {
                // scrapeNotices 내부에서 예외를 삼키지만, 혹시 모를 예외 대비
                log.error("Scraping failed for type: {}", type, e);
                failedTypes.add(type);
            }
        });

        if (!failedTypes.isEmpty()) {
            log.info(">>> 스크래핑 실패 카테고리: {}", failedTypes);
            // 선택: 즉시 1회 재시도
            for (String type : failedTypes) {
                String url = noticeConfig.getUrls().get(type);
                try {
                    Thread.sleep(1000);
                    noticeScrapeService.scrapeNotices(url, type);
                    log.info("[retry] Scraping success for type: {}", type);
                } catch (Exception e) {
                    log.error("[retry] Scraping failed again for type: {}", type, e);
                }
            }
        }
    }
}
