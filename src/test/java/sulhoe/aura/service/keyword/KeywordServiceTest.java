package sulhoe.aura.service.keyword;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.util.ReflectionTestUtils;
import sulhoe.aura.entity.Keyword;
import sulhoe.aura.entity.Keyword.Scope;
import sulhoe.aura.entity.Notice;
import sulhoe.aura.repository.KeywordRepository;
import sulhoe.aura.repository.NoticeRepository;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KeywordServiceTest {

    @Mock
    private KeywordRepository keywordRepo;

    @Mock
    private NoticeRepository noticeRepo;

    @Mock
    private sulhoe.aura.repository.UserRepository userRepo;

    @Mock
    private sulhoe.aura.repository.UserTypePreferenceRepository utpRepo;

    @Mock
    private sulhoe.aura.repository.UserTypeKeywordRepository utikRepo;

    @Mock
    private sulhoe.aura.service.firebase.PushNotificationService push;

    @Mock
    private sulhoe.aura.config.NoticeConfig noticeConfig;

    private KeywordService keywordService;

    private Keyword global1;
    private Keyword global2;

    @BeforeEach
    void setUp() {
        keywordService = new KeywordService(
                keywordRepo,
                noticeRepo,
                userRepo,
                push,
                noticeConfig,
                utpRepo,
                utikRepo
        );

        global1 = Keyword.builder().id(1L).phrase("공모전").scope(Scope.GLOBAL).build();
        global2 = Keyword.builder().id(2L).phrase("장학금").scope(Scope.GLOBAL).build();
    }

    @Test
    void onNoticeSaved_usesCachedGlobals_notDbQuery() {
        ReflectionTestUtils.setField(keywordService, "cachedGlobalNorms",
                Set.of("공모전", "장학금"));

        Notice notice = new Notice("123", "test", "2024 장학금 공모전 안내", "학과", "2024-01-01", "http://example.com");
        UUID noticeId = UUID.randomUUID();
        notice.setId(noticeId);
        when(noticeRepo.findByIdWithKeywords(noticeId)).thenReturn(java.util.Optional.of(notice));
        when(utpRepo.findAllUserIdsByTypeAndAll("test")).thenReturn(List.of());

        keywordService.onNoticeSaved(notice, "test");

        verify(keywordRepo, never()).findAllByScope(Scope.GLOBAL);
    }

    @Test
    void onNoticeSaved_dbQueryOnlyWhenCacheNull() {
        ReflectionTestUtils.setField(keywordService, "cachedGlobalNorms", null);
        when(keywordRepo.findAllByScope(Scope.GLOBAL)).thenReturn(List.of(global1, global2));

        Notice notice = new Notice("123", "test", "2024 장학금 공모전 안내", "학과", "2024-01-01", "http://example.com");
        UUID noticeId = UUID.randomUUID();
        notice.setId(noticeId);
        when(noticeRepo.findByIdWithKeywords(noticeId)).thenReturn(java.util.Optional.of(notice));
        when(utpRepo.findAllUserIdsByTypeAndAll("test")).thenReturn(List.of());

        keywordService.onNoticeSaved(notice, "test");

        verify(keywordRepo, times(1)).findAllByScope(Scope.GLOBAL);
    }

    @Test
    void refreshGlobalCache_queriesDbOnce() {
        when(keywordRepo.findAllByScope(Scope.GLOBAL)).thenReturn(List.of(global1, global2));

        keywordService.refreshGlobalCache();

        verify(keywordRepo, times(1)).findAllByScope(Scope.GLOBAL);
    }

    @Test
    void cachedGlobalNorms_containsNormalizedPhrases() {
        when(keywordRepo.findAllByScope(Scope.GLOBAL)).thenReturn(List.of(global1, global2));

        keywordService.refreshGlobalCache();

        Object cached = ReflectionTestUtils.getField(keywordService, "cachedGlobalNorms");
        assertThat(cached).isInstanceOf(Set.class);
    }
}