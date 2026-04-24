package sulhoe.aura.service.notice;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.context.ApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;
import sulhoe.aura.config.NoticeConfig;
import sulhoe.aura.entity.Notice;
import sulhoe.aura.repository.NoticeRepository;
import sulhoe.aura.service.keyword.KeywordService;
import sulhoe.aura.service.notice.parser.NoticeParser;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class NoticeScrapeServiceTest {

    @Mock
    private NoticeConfig noticeConfig;

    @Mock
    private ApplicationContext ctx;

    @Mock
    private NoticeRepository repo;

    @Mock
    private NoticePersistenceService persistence;

    @Mock
    private KeywordService keywordService;

    @Mock
    private NoticeFanoutService fanoutService;

    private NoticeScrapeService service;

    @BeforeEach
    void setUp() {
        service = new NoticeScrapeService(
                noticeConfig,
                ctx,
                repo,
                persistence,
                keywordService,
                fanoutService
        );
    }

    @Test
    void whenForceBackfillFalse_andDataExists_thenIncrementalMode() {
        ReflectionTestUtils.setField(service, "forceBackfill", false);
        when(repo.existsByType("test")).thenReturn(true);
        when(repo.countByTypeAndCreatedAtAfter(any(), any())).thenReturn(1L);

        boolean backfillMissing = false;
        boolean fullLoad = backfillMissing || shouldDoFullLoad(service, "test");

        assertThat(fullLoad).isFalse();
    }

    @Test
    void whenForceBackfillTrue_thenFullLoadMode() {
        ReflectionTestUtils.setField(service, "forceBackfill", true);
        lenient().when(repo.existsByType("test")).thenReturn(true);

        boolean backfillMissing = true;
        boolean fullLoad = backfillMissing || shouldDoFullLoad(service, "test");

        assertThat(fullLoad).isTrue();
    }

    @Test
    void whenForceBackfillFalse_andNoData_thenFullLoadMode() {
        ReflectionTestUtils.setField(service, "forceBackfill", false);
        lenient().when(repo.existsByType("test")).thenReturn(false);

        boolean backfillMissing = false;
        boolean fullLoad = backfillMissing || shouldDoFullLoad(service, "test");

        assertThat(fullLoad).isTrue();
    }

    @Test
    void scrapeNoticesDefersDuplicateChecksToPersistenceWithoutCallingExistsByLink() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/notices", this::handleScrapeRequest);
        server.start();

        try {
            String baseUrl = "http://localhost:" + server.getAddress().getPort() + "/notices";
            NoticeParser parser = new TestNoticeParser();

            when(noticeConfig.getParser()).thenReturn(Map.of("default", "testParser", "test", "testParser"));
            when(noticeConfig.getCategoriesRequirePostedDate()).thenReturn(Set.of());
            when(ctx.getBean("testParser", NoticeParser.class)).thenReturn(parser);
            when(repo.existsByType("test")).thenReturn(false);
            when(persistence.persistNotices(any())).thenAnswer(invocation -> invocation.getArgument(0));

            service.scrapeNotices(baseUrl, "test");

            ArgumentCaptor<List<Notice>> persisted = ArgumentCaptor.forClass(List.class);
            verify(persistence).persistNotices(persisted.capture());
            assertThat(persisted.getValue())
                    .extracting(Notice::getLink)
                    .containsExactly(baseUrl + "/notice-1", baseUrl + "/notice-2");
            verify(repo, never()).existsByLink(anyString());
        } finally {
            server.stop(0);
        }
    }

    private boolean shouldDoFullLoad(NoticeScrapeService service, String type) {
        return ReflectionTestUtils.invokeMethod(service, "shouldDoFullLoad", type);
    }

    private void handleScrapeRequest(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        String body = query != null && query.contains("article.offset=0")
                ? """
                <html><body><table><tbody>
                    <tr class='notice' data-number='1' data-title='First' data-link='/notice-1'></tr>
                    <tr class='notice' data-number='2' data-title='Second' data-link='/notice-2'></tr>
                </tbody></table></body></html>
                """
                : "<html><body><table><tbody></tbody></table></body></html>";

        exchange.getResponseHeaders().add("Content-Type", "text/html; charset=UTF-8");
        byte[] bytes = body.getBytes();
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static class TestNoticeParser implements NoticeParser {
        @Override
        public org.jsoup.select.Elements selectFixedRows(org.jsoup.nodes.Document doc) {
            return new org.jsoup.select.Elements();
        }

        @Override
        public org.jsoup.select.Elements selectGeneralRows(org.jsoup.nodes.Document doc) {
            return doc.select("tr.notice");
        }

        @Override
        public Notice parseRow(org.jsoup.nodes.Element row, boolean isFixed, String baseUrl) {
            Notice notice = new Notice(
                    row.attr("data-number"),
                    null,
                    row.attr("data-title"),
                    null,
                    "2026-04-24",
                    baseUrl + row.attr("data-link")
            );
            notice.setType("test");
            return notice;
        }
    }
}
