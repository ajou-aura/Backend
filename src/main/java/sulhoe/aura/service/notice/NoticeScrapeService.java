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
import java.net.SocketTimeoutException;
import java.util.concurrent.ThreadLocalRandom;

@Service
@RequiredArgsConstructor
public class NoticeScrapeService {
    private static final Logger logger = LoggerFactory.getLogger(NoticeScrapeService.class);

    private static final int DEFAULT_TIMEOUT_MS = 30_000;
    private static final int RETRIES = 4;
    private static final int BASE_BACKOFF_MS = 600;
    private static final int MAX_BACKOFF_MS = 6_000;

    // ★ incremental 모드 시 최대 페이지만 제한
    private static final int INCREMENTAL_MAX_PAGES = 5;

    private final NoticeConfig noticeConfig;
    private final ApplicationContext ctx;
    private final PushNotificationService push;
    private final NoticeRepository repo;
    private final NoticePersistenceService persistence;
    private final KeywordService keywordService;

    private static int jitterBackoff(int attempt, int base, int max) {
        long exp = (long) (base * Math.pow(2, attempt - 1));
        long cap = Math.min(exp, max);
        int jitter = ThreadLocalRandom.current().nextInt((int)(cap * 0.4) + 1);
        return (int) (cap - jitter);
    }

    public void scrapeNotices(String url, String type) throws IOException {
        boolean fullLoad = shouldDoFullLoad(type);
        logger.info("[{}] ========== SCRAPE START: fullLoad={} ==========", type, fullLoad);

        String parserBean = noticeConfig.getParser().getOrDefault(type, noticeConfig.getParser().get("default"));
        NoticeParser parser = ctx.getBean(parserBean, NoticeParser.class);

        List<Notice> scraped = new ArrayList<>();

        // 1) 첫 페이지 고정 공지
        Document doc = fetchWithLog(url, type + ":first");
        Elements fixedRows = parser.selectFixedRows(doc);
        logger.info("[{}] Fixed notices: {}", type, fixedRows.size());

        for (Element row : fixedRows) {
            try {
                Notice n = parser.parseRow(row, true, url);
                n.setType(type);
                scraped.add(n);
            } catch (Exception ex) {
                logger.warn("[{}] Fixed row parse failed: {}", type, ex.toString());
            }
        }

        dumpOnce(type, 0, doc);

        // 2) 일반 페이지 루프
        int pageIdx = 0;
        int step = parser.getStep();
        int pagesFetched = 0;
        int consecutiveEmptyPages = 0;
        int consecutiveDuplicatePages = 0;  // ★ 연속 중복 페이지 카운터

        while (true) {
            String pagedUrl = parser.buildPageUrl(url, pageIdx);
            logger.debug("[{}] Fetching page {} (index: {})", type, pagesFetched + 1, pageIdx);

            Document pagedDoc = fetchWithLog(pagedUrl, type + ":page-" + pageIdx);
            Elements generalRows = parser.selectGeneralRows(pagedDoc);

            // ━━━━ 종료 조건 1: 빈 페이지 ━━━━
            if (generalRows.isEmpty()) {
                consecutiveEmptyPages++;
                logger.debug("[{}] Empty page (consecutive: {})", type, consecutiveEmptyPages);

                if (consecutiveEmptyPages >= 2) {
                    logger.info("[{}] ✓ End condition: 2 consecutive empty pages", type);
                    break;
                }

                pageIdx += step;
                pagesFetched++;
                continue;
            }

            consecutiveEmptyPages = 0;
            int pageNewCount = 0;
            int pageDuplicateCount = 0;

            // 행 파싱
            for (Element row : generalRows) {
                try {
                    Notice n = parser.parseRow(row, false, url);
                    n.setType(type);

                    // 중복 체크
                    if (repo.existsByLink(n.getLink())) {
                        pageDuplicateCount++;
                        continue;
                    }

                    scraped.add(n);
                    pageNewCount++;
                } catch (Exception ex) {
                    logger.warn("[{}] Row parse failed (page {}): {}", type, pagesFetched + 1, ex.toString());
                }
            }

            if (pagesFetched == 0) dumpOnce(type, 0, pagedDoc);

            pagesFetched++;
            pageIdx += step;

            logger.info("[{}] Page {}: {} new, {} duplicate (total scraped: {})",
                    type, pagesFetched, pageNewCount, pageDuplicateCount, scraped.size());

            // ━━━━ 종료 조건 2: Incremental 모드 - 페이지 제한 ━━━━
            if (!fullLoad && pagesFetched >= INCREMENTAL_MAX_PAGES) {
                logger.info("[{}] End condition: Incremental max pages ({})", type, INCREMENTAL_MAX_PAGES);
                break;
            }

            // ━━━━ 종료 조건 3: 연속 중복 페이지 (증분 수집 최적화) ━━━━
            if (pageNewCount == 0 && pageDuplicateCount > 0) {
                consecutiveDuplicatePages++;

                if (!fullLoad && consecutiveDuplicatePages >= 2) {
                    logger.info("[{}] End condition: 2 consecutive duplicate pages (incremental mode)", type);
                    break;
                }

                // fullLoad에서도 5페이지 연속 중복이면 종료
                if (fullLoad && consecutiveDuplicatePages >= 5) {
                    logger.info("[{}] End condition: 5 consecutive duplicate pages (full load)", type);
                    break;
                }
            } else {
                consecutiveDuplicatePages = 0;
            }

            // ━━━━ 종료 조건 4: 마지막 페이지 휴리스틱 (fullLoad만) ━━━━
            if (fullLoad) {
                int expectedPageSize = step == 1 ? 10 : step;

                if (generalRows.size() < expectedPageSize) {
                    logger.info("[{}] End condition: Last page detected (rows={} < expected={})",
                            type, generalRows.size(), expectedPageSize);
                    break;
                }
            }
        }

        logger.info("[{}] ========== SCRAPE END: {} pages, {} notices scraped ==========",
                type, pagesFetched, scraped.size());

        // 3) 작성일 별도 조회
        if (noticeConfig.getCategoriesRequirePostedDate().contains(type)) {
            logger.info("[{}] Fetching posted dates for {} notices...", type, scraped.size());
            for (Notice n : scraped) {
                n.setDate(fetchPostedDate(n.getLink()));
            }
        }

        // 4) DB 저장
        List<Notice> newOrUpdated = persistence.persistNotices(scraped);
        logger.info("[{}] FINAL: {} new/updated out of {} scraped ",
                type, newOrUpdated.size(), scraped.size());

        // 5) FCM 팬아웃
        for (Notice n : newOrUpdated) {
            keywordService.onNoticeSaved(n, type);
        }
    }

    /**
     * fullLoad 여부 판단
     * - DB에 해당 타입이 없으면 fullLoad
     * - 최근 7일 데이터가 없으면 fullLoad
     */
    private boolean shouldDoFullLoad(String type) {
        if (!repo.existsByType(type)) {
            logger.info("[{}] No data exists → Full load", type);
            return true;
        }

        java.time.LocalDateTime sevenDaysAgo = java.time.LocalDateTime.now().minusDays(7);
        long recentCount = repo.countByTypeAndCreatedAtAfter(type, sevenDaysAgo);

        if (recentCount == 0) {
            logger.info("[{}] No recent data (7 days) → Full load", type);
            return true;
        }

        logger.info("[{}] {} recent notices exist → Incremental load", type, recentCount);
        return false;
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

    private Document fetchWithLog(String url, String tag) throws IOException {
        IOException last = null;
        for (int attempt = 1; attempt <= RETRIES; attempt++) {
            try {
                Connection conn = Jsoup.connect(url)
                        .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                + "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
                        .referrer("https://www.google.com/")
                        .timeout(DEFAULT_TIMEOUT_MS)
                        .followRedirects(true)
                        .ignoreHttpErrors(true)
                        .maxBodySize(0)
                        .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                        .header("Accept-Encoding", "gzip,deflate,br")
                        .header("Accept-Language", "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7");

                Connection.Response resp = conn.execute();
                resp.bodyAsBytes();
                logger.debug("[fetch:{}] GET {} → {} ({} bytes)",
                        tag, url, resp.statusCode(), resp.bodyAsBytes().length);

                return resp.parse();
            } catch (SocketTimeoutException e) {
                last = e;
                logger.warn("[fetch:{}] Timeout (attempt {}/{})", tag, attempt, RETRIES);
            } catch (IOException e) {
                last = e;
                logger.warn("[fetch:{}] Error (attempt {}/{}): {}", tag, attempt, RETRIES, e.toString());
            }

            int backoff = jitterBackoff(attempt, BASE_BACKOFF_MS, MAX_BACKOFF_MS);
            try { Thread.sleep(backoff); } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted during backoff", ie);
            }
        }
        throw last != null ? last : new IOException("Fetch failed: " + url);
    }

    private void dumpOnce(String type, int pageIdx, Document doc) {
        if (!(type.endsWith(".medicine") || type.endsWith(".nursing") || type.endsWith(".software"))) return;
        try {
            Path p = Path.of("/tmp/scrape-" + type.replace('.', '-') + "-p" + pageIdx + ".html");
            if (!Files.exists(p)) {
                Files.writeString(p, doc.outerHtml());
                logger.debug("[{}] Saved sample: {}", type, p);
            }
        } catch (Exception e) {
            logger.error("[{}] Failed to save sample: {}", type, e.toString());
        }
    }
}