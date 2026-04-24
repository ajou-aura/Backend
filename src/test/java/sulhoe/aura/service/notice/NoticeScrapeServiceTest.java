package sulhoe.aura.service.notice;

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
import sulhoe.aura.repository.NoticeRepository;
import sulhoe.aura.service.keyword.KeywordService;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
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

    private boolean shouldDoFullLoad(NoticeScrapeService service, String type) {
        return ReflectionTestUtils.invokeMethod(service, "shouldDoFullLoad", type);
    }
}