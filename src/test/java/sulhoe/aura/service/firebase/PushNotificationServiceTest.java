package sulhoe.aura.service.firebase;

import com.google.firebase.messaging.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import sulhoe.aura.service.notice.NoticeTypeLabelResolver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PushNotificationServiceTest {

    private NoticeTypeLabelResolver labelResolver;
    private FirebaseMessaging fmMock;
    private PushNotificationService service;

    @BeforeEach
    void setUp() {
        labelResolver = mock(NoticeTypeLabelResolver.class);
        when(labelResolver.labelOf(anyString())).thenReturn("학과");
        fmMock = mock(FirebaseMessaging.class);
        service = spy(new PushNotificationService(labelResolver));
        doReturn(fmMock).when(service).getFirebaseMessaging();
    }

    @Test
    void sendToUserTopics_callsSendEach_onceForMultipleUsers() throws Exception {
        List<String> emails = List.of("a@test.com", "b@test.com", "c@test.com");

        BatchResponse batchResponse = mock(BatchResponse.class);
        when(batchResponse.getFailureCount()).thenReturn(0);
        when(batchResponse.getSuccessCount()).thenReturn(3);

        when(fmMock.sendEach(anyList())).thenReturn(batchResponse);

        service.sendToUserTopics(emails, "test", "title", "http://link");

        verify(fmMock, times(1)).sendEach(anyList());
        verify(fmMock, never()).send(any(Message.class));
    }

    @Test
    void sendToUserTopics_passesAllMessagesInSingleBatch() throws Exception {
        List<String> emails = List.of("a@test.com", "b@test.com");

        BatchResponse batchResponse = mock(BatchResponse.class);
        when(batchResponse.getFailureCount()).thenReturn(0);
        when(batchResponse.getSuccessCount()).thenReturn(2);

        when(fmMock.sendEach(anyList())).thenReturn(batchResponse);

        service.sendToUserTopics(emails, "test", "title", "http://link");

        ArgumentCaptor<List<Message>> captor = ArgumentCaptor.forClass(List.class);
        verify(fmMock).sendEach(captor.capture());

        List<Message> sent = captor.getValue();
        assertThat(sent).hasSize(2);
    }

    @Test
    void sendToUserTopics_logsFailures_doesNotThrow() throws Exception {
        List<String> emails = List.of("a@test.com", "b@test.com");

        when(fmMock.sendEach(anyList())).thenThrow(FirebaseMessagingException.class);

        assertThatCode(() -> service.sendToUserTopics(emails, "test", "title", "http://link"))
                .doesNotThrowAnyException();
    }

    @Test
    void sendToUserTopics_emptyList_doesNotCallSendEach() throws Exception {
        service.sendToUserTopics(Collections.emptyList(), "test", "title", "http://link");

        verify(fmMock, never()).sendEach(anyList());
        verify(fmMock, never()).send(any(Message.class));
    }

    @Test
    void sendToUserTopics_batchesInGroupsOf500() throws Exception {
        List<String> emails = new ArrayList<>();
        for (int i = 0; i < 501; i++) {
            emails.add("user" + i + "@test.com");
        }

        BatchResponse batchResponse = mock(BatchResponse.class);
        when(batchResponse.getFailureCount()).thenReturn(0);
        when(batchResponse.getSuccessCount()).thenReturn(500);

        when(fmMock.sendEach(anyList())).thenReturn(batchResponse);

        service.sendToUserTopics(emails, "test", "title", "http://link");

        verify(fmMock, times(2)).sendEach(anyList());

        ArgumentCaptor<List<Message>> captor = ArgumentCaptor.forClass(List.class);
        verify(fmMock, times(2)).sendEach(captor.capture());

        List<List<Message>> allBatches = captor.getAllValues();
        assertThat(allBatches.get(0)).hasSize(500);
        assertThat(allBatches.get(1)).hasSize(1);
    }

    @Test
    void sendToUserTopics_skipsBlankEmails() throws Exception {
        List<String> emails = Arrays.asList("a@test.com", "", null, "  ", "b@test.com");

        BatchResponse batchResponse = mock(BatchResponse.class);
        when(batchResponse.getFailureCount()).thenReturn(0);
        when(batchResponse.getSuccessCount()).thenReturn(2);

        when(fmMock.sendEach(anyList())).thenReturn(batchResponse);

        service.sendToUserTopics(emails, "test", "title", "http://link");

        ArgumentCaptor<List<Message>> captor = ArgumentCaptor.forClass(List.class);
        verify(fmMock).sendEach(captor.capture());

        assertThat(captor.getValue()).hasSize(2);
    }
}