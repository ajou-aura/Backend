package sulhoe.aura.service.notice;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import sulhoe.aura.entity.Notice;
import sulhoe.aura.repository.NoticeRepository;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NoticePersistenceServiceTest {

    @Mock
    private NoticeRepository noticeRepo;

    private NoticePersistenceService service;

    @BeforeEach
    void setUp() {
        service = new NoticePersistenceService(noticeRepo);
    }

    @Test
    void persistNoticesUsesSingleBatchLookupAndSaveAllForNewNotices() {
        Notice first = notice("1", "First", "https://example.com/1");
        Notice second = notice("2", "Second", "https://example.com/2");

        when(noticeRepo.findByLinkIn(List.of(first.getLink(), second.getLink()))).thenReturn(List.of());
        when(noticeRepo.saveAll(anyList())).thenAnswer(invocation -> invocation.getArgument(0));

        List<Notice> persisted = service.persistNotices(List.of(first, second));

        assertThat(persisted).containsExactly(first, second);
        verify(noticeRepo).findByLinkIn(List.of(first.getLink(), second.getLink()));

        ArgumentCaptor<List<Notice>> savedBatch = ArgumentCaptor.forClass(List.class);
        verify(noticeRepo).saveAll(savedBatch.capture());
        assertThat(savedBatch.getValue()).containsExactly(first, second);
        verify(noticeRepo, never()).findByLink(anyString());
    }

    @Test
    void saveOrUpdateOneKeepsIndividualUpdatePathForSingleNoticeSaves() {
        Notice existing = notice("1", "Old", "https://example.com/1");
        existing.setType("bachelor");
        existing.setDate("2026-04-23");

        Notice incoming = notice("2", "New", "https://example.com/1");
        incoming.setType("graduate");
        incoming.setDate("2026-04-24");

        when(noticeRepo.findByLink(incoming.getLink())).thenReturn(Optional.of(existing));
        when(noticeRepo.save(existing)).thenReturn(existing);

        Notice saved = service.saveOrUpdateOne(incoming);

        assertThat(saved).isSameAs(existing);
        assertThat(existing.getType()).isEqualTo("graduate");
        assertThat(existing.getTitle()).isEqualTo("New");
        assertThat(existing.getDate()).isEqualTo("2026-04-24");
        assertThat(existing.getNumber()).isEqualTo("2");
        verify(noticeRepo).findByLink(incoming.getLink());
        verify(noticeRepo).save(existing);
    }

    private Notice notice(String number, String title, String link) {
        Notice notice = new Notice(number, null, title, null, "2026-04-24", link);
        notice.setType("test");
        return notice;
    }
}
