package sulhoe.aura.service.firebase;

import com.google.firebase.messaging.AndroidConfig;
import com.google.firebase.messaging.FirebaseMessaging;
import com.google.firebase.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.text.Normalizer;
import java.time.Duration;
import java.util.Locale;

@Service
public class PushNotificationService {
    private static final Logger logger = LoggerFactory.getLogger(PushNotificationService.class);

    // 공통 규칙: title = "[type] 새로운 공지사항이 게시되었습니다.", body = 공지 제목
    private static Payload buildPayload(String type, String noticeTitle) {
        String t = nz(type);
        String header = "[" + (t.isBlank() ? "unknown" : t) + "] 새로운 공지사항이 게시되었습니다.";
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

    // email 기발 사용자 토픽 (/topics/user-{sanitize(email)})
    public void sendToUserTopic(String email, String type, String title, String link) {
        sendToTopic("user-" + sanitize(email), type, title, link);
    }

    // type 전체 구독 토픽 (/topics/type-{sanitized})
    public void sendToTypeTopic(String type, String title, String link) {
        sendToTopic("type-" + sanitize(type), type, title, link);
    }

    public void sendToTopic(String topic, String type, String title, String link) {
        try {
            Payload p = buildPayload(type, title); // title = 공지 제목
            Message message = Message.builder()
                    .setTopic(topic)
                    .putData("type", nz(type))
                    .putData("title", p.title())
                    .putData("body", p.body())
                    .putData("link", nz(link))
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

    // 최종 페이로드: title(고정 규칙), body(공지 제목)
    private record Payload(String title, String body) {
    }
}
