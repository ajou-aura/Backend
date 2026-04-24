package sulhoe.aura.service.firebase;

import com.google.firebase.messaging.AndroidConfig;
import com.google.firebase.messaging.AndroidNotification;
import com.google.firebase.messaging.ApnsConfig;
import com.google.firebase.messaging.Aps;
import com.google.firebase.messaging.ApsAlert;
import com.google.firebase.messaging.BatchResponse;
import com.google.firebase.messaging.FirebaseMessaging;
import com.google.firebase.messaging.Message;
import com.google.firebase.messaging.Notification;
import com.google.firebase.messaging.SendResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import sulhoe.aura.service.notice.NoticeTypeLabelResolver;

import java.text.Normalizer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

@Service
public class PushNotificationService {
    private static final Logger logger = LoggerFactory.getLogger(PushNotificationService.class);
    private static final int FCM_BATCH_SIZE = 500;

    private final NoticeTypeLabelResolver labelResolver;

    public PushNotificationService(NoticeTypeLabelResolver labelResolver) {
        this.labelResolver = labelResolver;
    }

    FirebaseMessaging getFirebaseMessaging() {
        return FirebaseMessaging.getInstance();
    }

    // 공통 규칙: title = "[type] 새로운 공지사항이 게시되었습니다.", body = 공지 제목
    private Payload buildPayload(String type, String noticeTitle) {
        String label = labelResolver.labelOf(type);
        String header = "[" + (label.isBlank() ? "알 수 없음" : label) + "] 새로운 공지사항이 게시되었습니다.";
        String body = nz(noticeTitle);
        return new Payload(header, body);
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }

    private static String sanitize(String t) {
        if (t == null) return "unknown";
        String s = Normalizer.normalize(t.trim(), Normalizer.Form.NFKC)
                .toLowerCase(Locale.ROOT)
                .replaceAll("\\s+", "-")
                .replaceAll("[^a-z0-9-_.~%]", "-");
        return s.replaceAll("-{2,}", "-");
    }

    public void sendToUserTopics(List<String> emails, String type, String title, String link) {
        List<String> valid = emails.stream()
                .filter(e -> e != null && !e.isBlank())
                .toList();
        if (valid.isEmpty()) return;

        Payload p = buildPayload(type, title);
        List<Message> messages = valid.stream()
                .map(email -> buildTopicMessage("user-" + sanitize(email), type, title, link, p))
                .toList();

        sendInBatches(messages);
    }

    public void sendToUserTopic(String email, String type, String title, String link) {
        sendToTopic("user-" + sanitize(email), type, title, link);
    }

    public void sendToTypeTopic(String type, String title, String link) {
        sendToTopic("type-" + sanitize(type), type, title, link);
    }

    public void sendToTopic(String topic, String type, String title, String link) {
        try {
            Payload p = buildPayload(type, title);
            Message message = buildTopicMessage(topic, type, title, link, p);
            String response = getFirebaseMessaging().send(message);
            logger.info("FCM 전송 완료: topic={}, {}", topic, response);
        } catch (Exception e) {
            logger.error("FCM 전송 실패 (topic=" + topic + ")", e);
        }
    }

    private Message buildTopicMessage(String topic, String type, String title, String link, Payload p) {
        return Message.builder()
                .setTopic(topic)
                .putData("type", nz(type))
                .putData("title", p.title())
                .putData("body", p.body())
                .putData("link", nz(link))
                .setNotification(Notification.builder()
                        .setTitle(p.title())
                        .setBody(p.body())
                        .build())
                .setAndroidConfig(androidHighPriority(Duration.ofHours(6), p))
                .setApnsConfig(apnsAlertConfig(Duration.ofHours(6), p))
                .build();
    }

    private void sendInBatches(List<Message> messages) {
        for (int i = 0; i < messages.size(); i += FCM_BATCH_SIZE) {
            List<Message> batch = messages.subList(i, Math.min(i + FCM_BATCH_SIZE, messages.size()));
            try {
                BatchResponse response = getFirebaseMessaging().sendEach(batch);
                logBatchFailures(response);
            } catch (Exception e) {
                logger.error("FCM 배치 전송 실패 (batch offset={})", i, e);
            }
        }
    }

    private void logBatchFailures(BatchResponse response) {
        int failures = response.getFailureCount();
        if (failures == 0) {
            logger.info("FCM 배치 전송 완료: {}건 성공", response.getSuccessCount());
            return;
        }
        for (SendResponse sr : response.getResponses()) {
            if (!sr.isSuccessful()) {
                logger.warn("FCM 개별 전송 실패: messageId={}, error={}",
                        sr.getMessageId(),
                        sr.getException() != null ? sr.getException().getMessage() : "unknown");
            }
        }
        logger.warn("FCM 배치 부분 실패: {}건 성공, {}건 실패",
                response.getSuccessCount(), response.getFailureCount());
    }

    private AndroidConfig androidHighPriority(Duration ttl, Payload payload) {
        return AndroidConfig.builder()
                .setPriority(AndroidConfig.Priority.HIGH)
                .setTtl(ttl.toMillis())
                .setNotification(AndroidNotification.builder()
                        .setTitle(payload.title())
                        .setBody(payload.body())
                        .setSound("default")
                        .build())
                .build();
    }

    private ApnsConfig apnsAlertConfig(Duration ttl, Payload payload) {
        return ApnsConfig.builder()
                .putHeader("apns-priority", "10")
                .putHeader("apns-push-type", "alert")
                .putHeader("apns-expiration", String.valueOf(Instant.now().plus(ttl).getEpochSecond()))
                .setAps(Aps.builder()
                        .setAlert(ApsAlert.builder()
                                .setTitle(payload.title())
                                .setBody(payload.body())
                                .build())
                        .setSound("default")
                        .setContentAvailable(true)
                        .build())
                .build();
    }

    // 최종 페이로드: title(고정 규칙), body(공지 제목)
    private record Payload(String title, String body) {
    }
}
