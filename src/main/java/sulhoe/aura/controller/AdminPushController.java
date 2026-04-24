// src/main/java/sulhoe/aura/controller/AdminPushController.java
package sulhoe.aura.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import sulhoe.aura.dto.ApiResponse;
import sulhoe.aura.entity.Keyword;
import sulhoe.aura.entity.User;
import sulhoe.aura.repository.KeywordRepository;
import sulhoe.aura.repository.UserRepository;
import sulhoe.aura.repository.UserTypeKeywordRepository;
import sulhoe.aura.service.firebase.PushNotificationService;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/admin/push")
@RequiredArgsConstructor
public class AdminPushController {

    private final PushNotificationService push;
    private final UserRepository userRepo;
    private final KeywordRepository keywordRepo;
    private final UserTypeKeywordRepository utikRepo; // 추가

    // title 필드는 "공지 제목"으로 해석됩니다. (서버가 최종 title/body 포맷을 강제 적용)
    public record TopicReq(String topic, String type, String title, String link) {}
    public record EmailsReq(List<String> emails, String type, String title, String link) {}
    public record KeywordsReq(List<Long> keywordIds, String type, String title, String link) {}

    /* ===== 1) 임의 토픽으로 발송 ===== */
    @PostMapping("/topic")
    public ResponseEntity<ApiResponse<Void>> sendToTopic(@RequestBody TopicReq req) {
        String topic = (req.topic() == null || req.topic().isBlank()) ? "notices" : req.topic().trim();
        push.sendToTopic(topic, req.type(), req.title(), req.link());
        return ResponseEntity.ok(ApiResponse.success(null));
    }

    /* ===== 2) 특정 사용자(Emails)에게 발송 ===== */
    @PostMapping("/users")
    public ResponseEntity<ApiResponse<Map<String, Object>>> sendToUsers(@RequestBody EmailsReq req) {
        if (req.emails() == null || req.emails().isEmpty()) {
            return ResponseEntity.badRequest().body(ApiResponse.error(400, "emails가 비어있습니다.", null));
        }
        // 중복/공백 제거 + 존재 사용자만 필터링(선택)
        Set<String> uniq = req.emails().stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        int sent = 0;
        for (String email : uniq) {
            if (!userRepo.existsByEmail(email)) continue;
            push.sendToUserTopic(email, req.type(), req.title(), req.link());
            sent++;
        }
        return ResponseEntity.ok(ApiResponse.success(Map.of(
                "requested", req.emails().size(),
                "unique", uniq.size(),
                "sent", sent
        )));
    }

    // 3) type 내 특정 키워드 구독자들에게 발송 (uid -> email 변환 후 전송)
    @PostMapping("/keywords")
    public ResponseEntity<ApiResponse<Map<String, Object>>> sendToKeywordSubscribers(
            @RequestBody KeywordsReq req) {
        if (req.keywordIds() == null || req.keywordIds().isEmpty()) {
            return ResponseEntity.badRequest().body(
                    ApiResponse.error(400, "keywordIds가 비어있습니다.", null));
        }
        if (req.type() == null || req.type().isBlank()) {
            return ResponseEntity.badRequest().body(
                    ApiResponse.error(400, "type이 비어있습니다.", null));
        }

        List<Keyword> keywords = keywordRepo.findAllById(req.keywordIds());
        List<Long> uidList = utikRepo.findUserIdsByTypeAndKeywordIds(req.type(), req.keywordIds());
        if (uidList.isEmpty()) {
            return ResponseEntity.ok(ApiResponse.success(Map.of(
                    "type", req.type(),
                    "keywords", keywords.stream().map(Keyword::getId).toList(),
                    "targets", 0,
                    "sent", 0
            )));
        }

        // uid → email 매핑
        List<User> users = userRepo.findAllById(new LinkedHashSet<>(uidList));
        List<String> emails = users.stream()
                .map(User::getEmail)
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .distinct()
                .toList();

        int sent = 0;
        for (String email : emails) {
            push.sendToUserTopic(email, req.type(), req.title(), req.link());
            sent++;
        }

        return ResponseEntity.ok(ApiResponse.success(Map.of(
                "type", req.type(),
                "keywords", keywords.stream().map(Keyword::getId).toList(),
                "targets", emails.size(),
                "sent", sent
        )));
    }
}
