package sulhoe.aura.service.notice;

import lombok.RequiredArgsConstructor;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import sulhoe.aura.config.NoticeConfig;
import sulhoe.aura.entity.Notice;
import sulhoe.aura.repository.NoticeRepository;
import sulhoe.aura.service.firebase.PushNotificationService;
import sulhoe.aura.service.notice.parser.NoticeParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import sulhoe.aura.service.keyword.KeywordService;

@Service
@RequiredArgsConstructor
public class NoticeScrapeService {
    private static final Logger logger = LoggerFactory.getLogger(NoticeScrapeService.class);

    private static final int DEFAULT_TIMEOUT_MS = 15_000;
    private static final int MAX_PAGES_CAP = 50;

    private final NoticeConfig noticeConfig;
    private final ApplicationContext ctx;
    private final PushNotificationService push;
    private final NoticeRepository repo;
    private final NoticePersistenceService persistence;
    private final KeywordService keywordService;

    public void scrapeNotices(String url, String type) throws IOException {
        boolean fullLoad = !repo.existsByType(type);
        logger.debug("[{}] scrapeNotices: fullLoad={}", type, fullLoad);
        logger.debug("[Debug] Start scraping: {}", url);

        // 1) 파서 선택
        String parserBean = noticeConfig.getParser().getOrDefault(type, noticeConfig.getParser().get("default"));
        logger.debug("[Info] Parser: {}", parserBean);
        NoticeParser parser = ctx.getBean(parserBean, NoticeParser.class);

        List<Notice> scraped = new ArrayList<>();

        // 2) 첫 페이지(고정 공지)
        Document doc = fetchWithLog(url, type + ":first");
        Elements fixedRows = parser.selectFixedRows(doc);
        logger.debug("[{}] fixedRows={}", type, fixedRows.size());

        for (Element row : fixedRows) {
            try {
                Notice n = parser.parseRow(row, true, url);
                n.setType(type);
                scraped.add(n);
            } catch (Exception ex) {
                logger.warn("[{}] fixed row parse failed: {}\nRowHTML={}", type, ex.toString(), row.outerHtml());
            }
        }

        // 문제 유형 HTML 샘플 1회 저장
        dumpOnce(type, 0, doc);

        // 3) 일반 페이지 루프
        int pageIdx = 0;
        int step = parser.getStep();
        int pagesFetched = 0;

        while (true) {
            String pagedUrl = parser.buildPageUrl(url, pageIdx);
            logger.debug("[{}] Scraping page: {}", type, pagedUrl);

            Document pagedDoc = fetchWithLog(pagedUrl, type + ":page-" + pageIdx);
            Elements generalRows = parser.selectGeneralRows(pagedDoc);
            logger.debug("[{}] generalRows(pageIdx={})={}", type, pageIdx, generalRows.size());

            if (generalRows.isEmpty()) {
                logger.debug("[{}] No more rows at pageIdx {}; breaking.", type, pageIdx);
                break;
            }

            for (Element row : generalRows) {
                try {
                    Notice n = parser.parseRow(row, false, url);
                    n.setType(type);
                    scraped.add(n);
                } catch (Exception ex) {
                    logger.warn("[{}] general row parse failed (pageIdx={}): {}\nRowHTML={}",
                            type, pageIdx, ex.toString(), row.outerHtml());
                }
            }

            // 첫 페이지 샘플 저장(문제 유형만)
            if (pagesFetched == 0) dumpOnce(type, 0, pagedDoc);

            pagesFetched++;
            pageIdx += step;

            if (pagesFetched >= MAX_PAGES_CAP) {
                logger.warn("[{}] Page cap({}) reached. Breaking.", type, MAX_PAGES_CAP);
                break;
            }

            if (!fullLoad && pagesFetched >= 3) { // 운영 중엔 3 페이지만
                logger.debug("[{}] Not fullLoad; only first pages fetched ({} pages). Breaking.", type, pagesFetched);
                break;
            }

            // 마지막 페이지 휴리스틱(가져온 행 수가 step 미만이면 종료)
            if (fullLoad && generalRows.size() < step) {
                logger.debug("[{}] Last page detected (rows < step). Breaking.", type);
                break;
            }
        }

        // 4) 작성일 별도 조회
        if (noticeConfig.getCategoriesRequirePostedDate().contains(type)) {
            for (Notice n : scraped) {
                n.setDate(fetchPostedDate(n.getLink()));
            }
        }

        // 5) DB 저장
        List<Notice> newOrUpdated = persistence.persistNotices(scraped);
        logger.debug("[{}] New/Updated count: {}", type, newOrUpdated.size());

        // 5-1) 새로 저장(또는 업데이트)된 공지들에 대해: 전역 키워드 태깅 + FCM 타겟 발송
        for (Notice n : newOrUpdated) {
            keywordService.onNoticeSaved(n, type);
        }

        // 6) 운영 중 알림(기존 토픽 알림은 필요 시 유지)
        if (!fullLoad && !newOrUpdated.isEmpty()) {
            // 기존 전체 토픽 발송을 원하면 주석 해제:
            // push.sendPushNotification(NoticeDto.toDtoList(newOrUpdated), type);
        }
    }

    private String fetchPostedDate(String link) {
        try {
            Document detailDoc = fetchWithLog(link, "detail");
            Element dateElement = detailDoc.selectFirst("li.b-date-box span:contains(작성일) + span");
            return dateElement != null ? dateElement.text() : "Unknown";
        } catch (IOException e) {
            logger.error("fetchPostedDate failed: {}", link, e);
            return "Unknown";
        }
    }

    // Jsoup 공통 페치 & 로깅
    private Document fetchWithLog(String url, String tag) throws IOException {
        Connection conn = Jsoup.connect(url)
                .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        + "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
                .referrer("https://www.google.com/")
                .timeout(DEFAULT_TIMEOUT_MS)
                .followRedirects(true)
                .ignoreHttpErrors(true);

        Connection.Response resp = conn.execute();
        logger.debug("[fetch:{}] GET {} -> status={}, contentType={}, finalUrl={}",
                tag, url, resp.statusCode(), resp.contentType(), resp.url());
        return resp.parse();
    }

    private void dumpOnce(String type, int pageIdx, Document doc) {
        // 문제 발생 유형만 샘플 저장: medicine/nursing/software
        if (!(type.endsWith(".medicine") || type.endsWith(".nursing") || type.endsWith(".software"))) return;
        try {
            Path p = Path.of("/tmp/scrape-" + type.replace('.', '-') + "-p" + pageIdx + ".html");
            if (!Files.exists(p)) {
                Files.writeString(p, doc.outerHtml());
                logger.debug("[{}] Saved sample: {}", type, p);
            }
        } catch (Exception e) {
            logger.error("[{}] Failed to save sample html: {}", type, e.toString());
        }
    }
}
