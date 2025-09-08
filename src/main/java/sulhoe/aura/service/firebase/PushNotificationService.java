package sulhoe.aura.service.firebase;

import com.google.firebase.messaging.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import sulhoe.aura.dto.notice.NoticeDto;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

@Service
public class PushNotificationService {
    private static final Logger logger = LoggerFactory.getLogger(PushNotificationService.class);

    // 기존: 전체 토픽(notices) 팬아웃
    public void sendPushNotification(List<NoticeDto> newNotices, String type) {
        for (NoticeDto notice : newNotices) {
            sendToTopic("notices", type, notice.title(), notice.link());
        }
    }

    /** 사용자별 토픽 (/topics/user-{id}) — 민감하지 않은 개인 알림에 한해 사용 권장 */
    public void sendToUserTopic(Long userId, String type, String title, String link) {
        sendToTopic("user-" + userId, type, title, link);
    }

    /** 다수 디바이스 토큰으로 배치 전송(선택 기능) */
    public void sendToTokens(Collection<String> tokens, String type, String title, String link) {
        if (tokens == null || tokens.isEmpty()) return;
        MulticastMessage msg = MulticastMessage.builder()
                .putData("type", nullToEmpty(type))
                .putData("link", nullToEmpty(link))
                .putData("body", nullToEmpty(title))
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

    /** 토픽 전송(기본 data-only) + Android 우선순위/TTL 옵션 적용 */
    public void sendToTopic(String topic, String type, String title, String link) {
        try {
            Message message = Message.builder()
                    .setTopic(topic)
                    // 필요 시 Notification도 함께 넣어 백그라운드 표시 품질 향상
                    // .setNotification(Notification.builder().setTitle(title).setBody(title).build())
                    .putData("type", nullToEmpty(type))
                    .putData("link", nullToEmpty(link))
                    .putData("body", nullToEmpty(title))
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
                .setPriority(AndroidConfig.Priority.HIGH)   // 사용자 가시 알림이면 HIGH 권장
                .setTtl(ttl.toMillis())                     // 시간 민감도에 맞춰 조정
                .build();
    }

    private String nullToEmpty(String s) { return s == null ? "" : s; }
}
