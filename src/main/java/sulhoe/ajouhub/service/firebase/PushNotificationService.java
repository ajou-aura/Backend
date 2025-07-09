package sulhoe.ajouhub.service.firebase;

import com.google.firebase.messaging.FirebaseMessaging;
import com.google.firebase.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import sulhoe.ajouhub.dto.notice.NoticeDto;

import java.util.List;

@Service
public class PushNotificationService {
    private static final Logger logger = LoggerFactory.getLogger(PushNotificationService.class);

    // 새로운 공지사항 리스트와 타입 받아서 FCM 토픽으로 메시지를 전송
    public void sendPushNotification(List<NoticeDto> newNotices, String type) {
        for (NoticeDto notice : newNotices) {
            // 한 건씩 알림, “type” 필드를 data에 넣어 앱이 어떤 페이지로 갈지 결정
            sendToFirebaseAdminSdk(type, notice.title(), notice.link());
        }
    }

    // Admin SDK 방식으로 푸시 메시지 전송
    private void sendToFirebaseAdminSdk(String type, String title, String link) {
        try {
            // 토픽으로 발송: "notices" 토픽
            Message message = Message.builder()
                    .setTopic("notices")  // /topics/notices
                    .putData("type", type)
                    .putData("link", link) // data 필드 추가
                    .putData("body", title)
                    .build();

            // 메시지 전송
            String response = FirebaseMessaging.getInstance().send(message);
            logger.info("FCM 전송 완료: {}", response);
        } catch (Exception e) {
            logger.error("FCM 전송 실패", e);
        }
    }
}