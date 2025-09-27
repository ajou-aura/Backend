package sulhoe.aura.service.firebase;

import com.google.firebase.messaging.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import sulhoe.aura.dto.notice.NoticeDto;

import java.text.Normalizer;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

@Service
public class PushNotificationService {
    private static final Logger logger = LoggerFactory.getLogger(PushNotificationService.class);

    public void sendPushNotification(List<NoticeDto> newNotices, String type) {
        if (newNotices == null || newNotices.isEmpty()) {
            logger.info("No notices to send");
            return;
        }
        for (NoticeDto notice : newNotices) {
            sendToTopic("notices", type, notice.title(), notice.link());
        }
    }

    /** 사용자별 토픽 (/topics/user-{id}) */
    public void sendToUserTopic(Long userId, String type, String title, String link) {
        sendToTopic("user-" + userId, type, title, link);
    }

    /** type 전체 구독 토픽 (/topics/type-{sanitized}) */
    public void sendToTypeTopic(String type, String title, String link) {
        sendToTopic("type-" + sanitize(type), type, title, link);
    }

    public void sendToTokens(Collection<String> tokens, String type, String title, String link) {
        if (tokens == null || tokens.isEmpty()) return;
        MulticastMessage msg = MulticastMessage.builder()
                .putData("type", nz(type))
                .putData("link", nz(link))
                .putData("body", nz(title))
                .addAllTokens(tokens)
                .setAndroidConfig(androidHighPriority(Duration.ofHours(6)))
                .build();
        try {
            BatchResponse resp = FirebaseMessaging.getInstance().sendEachForMulticast(msg);
            logger.info("FCM 배치 전송 완료: success={}, failure={}",
                    resp.getSuccessCount(), resp.getFailureCount());
        } catch (Exception e) {
            logger.error("FCM 배치 전송 실패", e);
        }
    }

    public void sendToTopic(String topic, String type, String title, String link) {
        try {
            Message message = Message.builder()
                    .setTopic(topic)
                    .putData("type", nz(type))
                    .putData("link", nz(link))
                    .putData("body", nz(title))
                    .setAndroidConfig(androidHighPriority(Duration.ofHours(6)))
                    .build();

            String response = FirebaseMessaging.getInstance().send(message);
            logger.info("FCM 전송 완료: topic={}, {}", topic, response);
        } catch (Exception e) {
            logger.error("FCM 전송 실패 (topic=" + topic + ")", e);
        }
    }

    private AndroidConfig androidHighPriority(Duration ttl) {
        return AndroidConfig.builder()
                .setPriority(AndroidConfig.Priority.HIGH)
                .setTtl(ttl.toMillis())
                .build();
    }

    private static String nz(String s) { return s == null ? "" : s; }

    private static String sanitize(String t) {
        if (t == null) return "unknown";
        String s = Normalizer.normalize(t.trim(), Normalizer.Form.NFKC)
                .toLowerCase(Locale.ROOT)
                .replaceAll("\\s+", "-")
                .replaceAll("[^a-z0-9-_.~%]", "-");
        return s.replaceAll("-{2,}", "-");
    }
}
