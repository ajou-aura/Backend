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
                log.info("Scraping success for type: {}", type);
            } catch (Exception e) {
                log.error("Scraping failed for type: {}", type, e);
                failedTypes.add(type);
            }
        });
        if (!failedTypes.isEmpty()) {
            log.warn(">>> 다음 카테고리에서 스크래핑에 실패했습니다: {}", failedTypes);
        }
    }

    // (선택) 매일 새벽 3시 수행
    // @Scheduled(cron = "0 0 3 * * ?")
    // public void scheduledMaintenanceTask() {
    //     // 예: 오래된 공지 삭제, 로그 정리 등
    // }
}
