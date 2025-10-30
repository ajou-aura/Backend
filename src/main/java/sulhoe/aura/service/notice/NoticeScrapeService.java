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
import sulhoe.aura.service.notice.parser.NoticeParser;

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

    // incremental 모드 시 최대 페이지만 제한
    private static final int INCREMENTAL_MAX_PAGES = 5;

    // 저장/팬아웃 청크 크기
    private static final int CHUNK_SIZE = 100;

    private final NoticeConfig noticeConfig;
    private final ApplicationContext ctx;
    private final NoticeRepository repo;
    private final NoticePersistenceService persistence;
    private final KeywordService keywordService;
    private final NoticeFanoutService fanoutService; // 팬아웃을 REQUIRES_NEW로 분리

    private static int jitterBackoff(int attempt, int base, int max) {
        long exp = (long) (base * Math.pow(2, attempt - 1));
        long cap = Math.min(exp, max);
        int jitter = ThreadLocalRandom.current().nextInt((int) (cap * 0.4) + 1);
        return (int) (cap - jitter);
    }

    /** 최상위에서 예외 전파하지 않음(스케줄러 계속 순회) */
    public void scrapeNotices(String url, String type) {
        try {
            boolean fullLoad = shouldDoFullLoad(type);
            logger.info("[{}] ========== SCRAPE START: fullLoad={} ==========", type, fullLoad);

            String parserBean = noticeConfig.getParser()
                    .getOrDefault(type, noticeConfig.getParser().get("default"));
            NoticeParser parser = ctx.getBean(parserBean, NoticeParser.class);

            // 버퍼(100건 단위로 flush)
            List<Notice> buffer = new ArrayList<>(CHUNK_SIZE);

            // 1) 첫 페이지 고정 공지
            Document doc = fetchWithLog(url, type + ":first");
            if (doc == null) {
                logger.warn("[{}] 첫 페이지 fetch 실패 → 이 타입은 이번 라운드 건너뜀", type);
                return;
            }
            Elements fixedRows = parser.selectFixedRows(doc);
            logger.info("[{}] Fixed notices: {}", type, fixedRows.size());

            for (Element row : fixedRows) {
                try {
                    Notice n = parser.parseRow(row, true, url);
                    n.setType(type);
                    buffer.add(n);
                    if (buffer.size() >= CHUNK_SIZE) {
                        flushChunk(buffer, type);
                    }
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
            int consecutiveDuplicatePages = 0;

            while (true) {
                String pagedUrl = parser.buildPageUrl(url, pageIdx);
                logger.debug("[{}] Fetching page {} (index: {})", type, pagesFetched + 1, pageIdx);

                Document pagedDoc = fetchWithLog(pagedUrl, type + ":page-" + pageIdx);
                if (pagedDoc == null) {
                    logger.warn("[{}] 페이지 fetch 실패(pageIdx={}): 다음 페이지로 계속", type, pageIdx);
                    pagesFetched++;
                    pageIdx += step;
                    continue;
                }
                Elements generalRows = parser.selectGeneralRows(pagedDoc);

                // 종료 조건 1: 빈 페이지 연속 2회
                if (generalRows.isEmpty()) {
                    consecutiveEmptyPages++;
                    logger.debug("[{}] Empty page (consecutive: {})", type, consecutiveEmptyPages);
                    if (consecutiveEmptyPages >= 2) {
                        logger.info("[{}] ✓ End: 2 consecutive empty pages", type);
                        break;
                    }
                    pageIdx += step;
                    pagesFetched++;
                    continue;
                }

                consecutiveEmptyPages = 0;
                int pageNewCount = 0;
                int pageDuplicateCount = 0;

                // 행 파싱(페이지 보호막과 별개로 행 단위 방어)
                try {
                    for (Element row : generalRows) {
                        try {
                            Notice n = parser.parseRow(row, false, url);
                            n.setType(type);

                            // 중복 체크
                            if (repo.existsByLink(n.getLink())) {
                                pageDuplicateCount++;
                                continue;
                            }

                            buffer.add(n);
                            pageNewCount++;
                            if (buffer.size() >= CHUNK_SIZE) {
                                flushChunk(buffer, type);
                            }
                        } catch (Exception ex) {
                            logger.warn("[{}] Row parse failed (page {}): {}", type, pagesFetched + 1, ex.toString());
                        }
                    }
                } catch (Exception pageEx) {
                    logger.warn("[{}] Page-level parse error (page {}): {}", type, pagesFetched + 1, pageEx.toString());
                }

                if (pagesFetched == 0) dumpOnce(type, 0, pagedDoc);

                pagesFetched++;
                pageIdx += step;

                logger.info("[{}] Page {}: {} new, {} duplicate (buffer now: {})",
                        type, pagesFetched, pageNewCount, pageDuplicateCount, buffer.size());

                // 종료 조건 2: 증분 모드 페이지 제한
                if (!fullLoad && pagesFetched >= INCREMENTAL_MAX_PAGES) {
                    logger.info("[{}] End: Incremental max pages ({})", type, INCREMENTAL_MAX_PAGES);
                    break;
                }

                // 종료 조건 3: 연속 중복 페이지
                if (pageNewCount == 0 && pageDuplicateCount > 0) {
                    consecutiveDuplicatePages++;
                    if (!fullLoad && consecutiveDuplicatePages >= 2) {
                        logger.info("[{}] End: 2 consecutive duplicate pages (incremental)", type);
                        break;
                    }
                    if (fullLoad && consecutiveDuplicatePages >= 5) {
                        logger.info("[{}] End: 5 consecutive duplicate pages (full load)", type);
                        break;
                    }
                } else {
                    consecutiveDuplicatePages = 0;
                }

                // 종료 조건 4: 마지막 페이지 휴리스틱(fullLoad만)
                if (fullLoad) {
                    int expectedPageSize = step == 1 ? 10 : step;
                    if (generalRows.size() < expectedPageSize) {
                        logger.info("[{}] End: Last page heuristic (rows={} < expected={})",
                                type, generalRows.size(), expectedPageSize);
                        break;
                    }
                }
            }

            // 남은 버퍼 flush
            if (!buffer.isEmpty()) flushChunk(buffer, type);

            logger.info("[{}] ========== SCRAPE END ==========", type);
        } catch (Exception e) {
            logger.error("[{}] scrapeNotices unexpected error (swallowed to continue schedule)", type, e);
        }
    }

    /** fullLoad 여부 판단 */
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

    /** 상세 페이지의 작성일 보강(실패 시 Unknown) */
    private String fetchPostedDate(String link) {
        try {
            Document detailDoc = fetchWithLog(link, "detail");
            if (detailDoc == null) return "Unknown";
            Element dateElement = detailDoc.selectFirst("li.b-date-box span:contains(작성일) + span");
            return dateElement != null ? dateElement.text() : "Unknown";
        } catch (Exception e) {
            logger.error("fetchPostedDate failed: {}", link, e);
            return "Unknown";
        }
    }

    /** 실패 시 null 반환(예외 전파 X) */
    private Document fetchWithLog(String url, String tag) {
        Exception last = null;
        for (int attempt = 1; attempt <= RETRIES; attempt++) {
            try {
                Connection conn = Jsoup.connect(url)
                        .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
                        .referrer("https://www.google.com/")
                        .timeout(DEFAULT_TIMEOUT_MS)
                        .followRedirects(true)
                        .ignoreHttpErrors(true)
                        .maxBodySize(0)
                        .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                        .header("Accept-Encoding", "gzip,deflate,br")
                        .header("Accept-Language", "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7");

                Connection.Response resp = conn.execute();
                byte[] body = resp.bodyAsBytes(); // 사이드이펙트: 실제 바디 로드
                logger.debug("[fetch:{}] GET {} → {} ({} bytes)", tag, url, resp.statusCode(), body.length);
                return resp.parse();
            } catch (SocketTimeoutException e) {
                last = e;
                logger.warn("[fetch:{}] Timeout (attempt {}/{})", tag, attempt, RETRIES);
            } catch (Exception e) {
                last = e;
                logger.warn("[fetch:{}] Error (attempt {}/{}): {}", tag, attempt, RETRIES, e.toString());
            }

            int backoff = jitterBackoff(attempt, BASE_BACKOFF_MS, MAX_BACKOFF_MS);
            try {
                Thread.sleep(backoff);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                logger.warn("[fetch:{}] Interrupted during backoff", tag);
                return null;
            }
        }
        logger.error("[fetch:{}] all retries failed: {} (last={})", tag, url, last != null ? last.toString() : "n/a");
        return null;
    }

    private void dumpOnce(String type, int pageIdx, Document doc) {
        if (!(type.endsWith(".medicine") || type.endsWith(".nursing") || type.endsWith(".software"))) return;
        try {
            if (doc == null) return;
            Path p = Path.of("/tmp/scrape-" + type.replace('.', '-') + "-p" + pageIdx + ".html");
            if (!Files.exists(p)) {
                Files.writeString(p, doc.outerHtml());
                logger.debug("[{}] Saved sample: {}", type, p);
            }
        } catch (Exception e) {
            logger.error("[{}] Failed to save sample: {}", type, e.toString());
        }
    }

    /** 버퍼(최대 100건)를 저장/팬아웃까지 처리 */
    private void flushChunk(List<Notice> buffer, String type) {
        if (buffer.isEmpty()) return;
        List<Notice> chunk = new ArrayList<>(buffer);
        buffer.clear();

        // 1) (옵션) 작성일 보강
        if (noticeConfig.getCategoriesRequirePostedDate().contains(type)) {
            for (Notice n : chunk) {
                try {
                    n.setDate(fetchPostedDate(n.getLink()));
                } catch (Exception e) {
                    logger.debug("[{}] posted-date enrich skipped for link={} ({})", type, n.getLink(), e.toString());
                }
            }
        }

        // 2) 저장(배치 실패해도 단건 진행되도록 내부에서 fail-soft)
        List<Notice> newOrUpdated;
        try {
            newOrUpdated = persistence.persistNotices(chunk);
            logger.info("[{}] CHUNK persisted: {} new/updated out of {}", type, newOrUpdated.size(), chunk.size());
        } catch (Exception e) {
            logger.error("[{}] CHUNK persist failed (skip this chunk): {}", type, e.toString(), e);
            return; // 저장 실패 시 팬아웃도 건너뜀
        }

        // 3) 팬아웃(새 트랜잭션, 실패해도 다음 항목/청크 진행)
        try {
            fanoutService.sendNotifications(newOrUpdated, type);
        } catch (Exception e) {
            // REQUIRES_NEW에서의 예외는 여기까지 오지 않는 것이 보통이지만 방어적으로 기록
            logger.error("[{}] fanout batch failed: {}", type, e.toString(), e);
        }
    }
}
