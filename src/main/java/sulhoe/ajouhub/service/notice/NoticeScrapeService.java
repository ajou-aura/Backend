package sulhoe.ajouhub.service.notice;

import lombok.RequiredArgsConstructor;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import sulhoe.ajouhub.config.NoticeConfig;
import sulhoe.ajouhub.dto.notice.NoticeDto;
import sulhoe.ajouhub.entity.Notice;
import sulhoe.ajouhub.repository.NoticeRepository;
import sulhoe.ajouhub.service.firebase.PushNotificationService;
import sulhoe.ajouhub.service.notice.parser.NoticeParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class NoticeScrapeService {
    private static final Logger logger = LoggerFactory.getLogger(NoticeScrapeService.class);

    private final NoticeConfig noticeConfig;
    private final ApplicationContext ctx;
    private final PushNotificationService push;
    private final NoticeRepository repo;
    private final NoticePersistenceService persistence;

    public void scrapeNotices(String url, String type) throws IOException {
        boolean fullLoad = !repo.existsByType(type); // 전체를 로드할 것인지: true 아닌지:false
        logger.info("[{}] scrapeNotices: fullLoad={}", type, fullLoad);
        logger.info("[Info] Start scraping: {}", url);

        // 1) 파서 선택
        String parserBean = noticeConfig.getParser().getOrDefault(type, noticeConfig.getParser().get("default"));
        logger.info("[Info] Parser: {}", parserBean);
        NoticeParser parser = ctx.getBean(parserBean, NoticeParser.class);

        List<Notice> scraped = new ArrayList<>();

        // 2) 첫 페이지(고정 공지)
        Document doc = Jsoup.connect(url).get();
        Elements fixedRows = parser.selectFixedRows(doc);
        fixedRows.forEach(row -> {
            Notice n = parser.parseRow(row, true, url);
            n.setType(type);
            scraped.add(n);
        });

        // 3) 일반 페이지 (최소 1회, fullLoad일 때만 계속)
        int pageIdx = 0;
        int step = parser.getStep();
        int pagesFetched = 0;
        while(true) {
            String pagedUrl = parser.buildPageUrl(url, pageIdx);
            logger.info("Scraping page: {}", pagedUrl);
            Document pagedDoc = Jsoup.connect(pagedUrl).get();
            Elements generalRows = parser.selectGeneralRows(pagedDoc);

            if (generalRows.isEmpty()) { // row가 아예 없으면 종료
                logger.info("No more rows at pageIdx {}; breaking.", pageIdx);
                break;
            }
            generalRows.forEach(row -> {
                Notice n = parser.parseRow(row, false, url);
                n.setType(type);
                scraped.add(n);
            });

            pagesFetched++;
            pageIdx += step;

            if (!fullLoad && pagesFetched >= 3) { // 전체 로드가 아니라면 1페이지만
                logger.info("Not fullLoad; only first page needed. Breaking.");
                break;
            }

            if (fullLoad && generalRows.size() < step) { // 마지막 페이지 판단: 가져온 row 수가 articleLimit 미만이면 종료
                logger.info("Last page detected (rows < 10). Breaking.");
                break;
            }
        }

        // 4) 작성일 별도 조회
        if (noticeConfig.getCategoriesRequirePostedDate().contains(type)) {
            scraped.forEach(n -> n.setDate(fetchPostedDate(n.getLink())));
        }

        // 5) DB 저장 ↔ 신규/업데이트된 리스트
        List<Notice> newOrUpdated = persistence.persistNotices(scraped);
        logger.info("[{}] New/Updated count: {}", type, newOrUpdated.size());

        // 6) 운영 중(=fullLoad=false) 알림
        if (!fullLoad && !newOrUpdated.isEmpty()) {
            // push.sendPushNotification(NoticeDto.toDtoList(newOrUpdated), type);
        }
    }

    private String fetchPostedDate(String link) {
        try {
            Document detailDoc = Jsoup.connect(link).get();
            Element dateElement = detailDoc.selectFirst("li.b-date-box span:contains(작성일) + span");
            return dateElement != null ? dateElement.text() : "Unknown";
        } catch (IOException e) {
            logger.error("fetchPostedDate failed: {}", link, e);
            return "Unknown";
        }
    }
}
