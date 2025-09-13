package sulhoe.aura.service.keyword;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import sulhoe.aura.entity.Keyword;
import sulhoe.aura.entity.Keyword.Scope;
import sulhoe.aura.entity.Notice;
import sulhoe.aura.entity.User;
import sulhoe.aura.repository.KeywordRepository;
import sulhoe.aura.repository.NoticeRepository;
import sulhoe.aura.repository.UserRepository;
import sulhoe.aura.service.firebase.PushNotificationService;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class KeywordService {

    private final KeywordRepository keywordRepo;
    private final NoticeRepository noticeRepo;
    private final UserRepository userRepo;
    private final PushNotificationService push;

    /* ===== 초기 시드 ===== */
    @Value("${app.keywords.seed:}") // 예: 공모전,장학,채용
    private String seed;

    @Value("${app.keywords.retag-on-start:false}")
    private boolean retagOnStart;

    // 부팅 시
    @EventListener(ApplicationReadyEvent.class)
    @Transactional
    public void initOnReady() {
        seedGlobalsIfNeeded();   // 아래로 분리
        if (retagOnStart) {
            retagAll();          // 아래로 분리
        }
    }

    @Transactional
    public void seedGlobalsIfNeeded() {
        if (seed != null && !seed.isBlank()) {
            Arrays.stream(seed.split(","))
                    .map(String::trim).filter(s -> !s.isBlank()).distinct()
                    .forEach(phrase -> {
                        if (!keywordRepo.existsByScopeAndPhraseIgnoreCase(Scope.GLOBAL, phrase)) {
                            keywordRepo.save(Keyword.builder()
                                    .phrase(phrase)
                                    .scope(Scope.GLOBAL)
                                    .build());
                        }
                    });
            log.info("[KeywordSeed] Seeded GLOBAL: {}", seed);
        }
    }

    @Transactional
    public void retagAll() {
        // 한 트랜잭션 안에서 반복 처리
        final List<Keyword> globals = keywordRepo.findAllByScope(Scope.GLOBAL);
        noticeRepo.findAll().forEach(n -> tagNoticeWithGlobalKeywords(n.getId(), globals));
    }

    // 기존 ID 버전은 리태깅 배치용으로 유지
    @Transactional
    public void tagNoticeWithGlobalKeywords(UUID noticeId, List<Keyword> globals) {
        Notice managed = noticeRepo.findById(noticeId).orElseThrow();
        tagNoticeWithGlobalKeywords(managed, globals);
    }

    /** 엔티티를 반드시 영속 상태로 다시 꺼내서 조작 */
    @Transactional
    public void tagNoticeWithGlobalKeywords(Notice managed, List<Keyword> globals) {
        final String title = Optional.ofNullable(managed.getTitle()).orElse("");

        Set<Keyword> matched = globals.stream()
                .filter(k -> containsIgnoreCase(title, k.getPhrase()))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        managed.getKeywords().clear();
        managed.getKeywords().addAll(matched);
        // 같은 영속성 컨텍스트 안이므로 save() 불필요
    }

    /* ===== 유틸 ===== */
    private static boolean containsIgnoreCase(String haystack, String needle) {
        if (haystack == null || needle == null) return false;
        String h = haystack.toLowerCase().replaceAll("\\s+", " ").trim();
        String n = needle.toLowerCase().replaceAll("\\s+", " ").trim();
        return !n.isEmpty() && h.contains(n);
    }

    /* ===== 개인 키워드 CRUD ===== */
    @Transactional
    public Keyword addMyKeyword(Long ownerId, String phrase) {
        if (keywordRepo.existsByOwnerIdAndPhraseIgnoreCase(ownerId, phrase)) {
            return keywordRepo.findAllByOwnerId(ownerId).stream()
                    .filter(k -> k.getPhrase().equalsIgnoreCase(phrase))
                    .findFirst().orElseThrow();
        }
        return keywordRepo.save(Keyword.builder()
                .phrase(phrase)
                .scope(Scope.USER)
                .ownerId(ownerId)
                .build());
    }

    @Transactional
    public void deleteMyKeyword(Long ownerId, Long keywordId) {
        Keyword k = keywordRepo.findByIdAndOwnerId(keywordId, ownerId).orElseThrow();
        keywordRepo.delete(k);
    }

    @Transactional(readOnly = true)
    public List<Keyword> listAllForUser(Long ownerId) {
        List<Keyword> res = new ArrayList<>();
        res.addAll(keywordRepo.findAllByScope(Scope.GLOBAL));
        res.addAll(keywordRepo.findAllByOwnerId(ownerId));
        return res;
    }

    /* ===== 전역 키워드 구독/해지 ===== */
    @Transactional
    public void subscribeGlobal(Long ownerId, Long globalKeywordId) {
        Keyword g = keywordRepo.findById(globalKeywordId).orElseThrow();
        if (g.getScope() != Scope.GLOBAL) throw new IllegalArgumentException("Only GLOBAL keyword can be subscribed.");
        User u = userRepo.findById(ownerId).orElseThrow();
        u.getSubscribedKeywords().add(g);
        userRepo.save(u);
    }

    @Transactional
    public void unsubscribeGlobal(Long ownerId, Long globalKeywordId) {
        User u = userRepo.findById(ownerId).orElseThrow();
        u.getSubscribedKeywords().removeIf(k -> Objects.equals(k.getId(), globalKeywordId));
        userRepo.save(u);
    }

    @Transactional(readOnly = true)
    public List<Long> mySubscriptionIds(Long ownerId) {
        return userRepo.findById(ownerId).orElseThrow()
                .getSubscribedKeywords().stream().map(Keyword::getId).toList();
    }

    /**
     * 저장 직후 호출: 태깅 + FCM 대상 계산 + 전송
     * @param detachedNotice    저장(신규/업데이트)된 Notice
     * @param type 기존 크롤러가 쓰던 라우팅용 타입
     */
    @Transactional
    public void onNoticeSaved(Notice detachedNotice, String type) {
        Notice n = noticeRepo.findByIdWithKeywords(detachedNotice.getId())
                .orElseThrow(() -> new IllegalStateException("Notice not found: " + detachedNotice.getId()));

        // 1) 전역 키워드 태깅
        final List<Keyword> globals = keywordRepo.findAllByScope(Scope.GLOBAL);
        tagNoticeWithGlobalKeywords(n.getId(), globals);

        // 2) FCM 대상 계산
        String title = Optional.ofNullable(n.getTitle()).orElse("");
        String link  = n.getLink();
        Set<Long> targets = new LinkedHashSet<>();

        // 2-a) 전역 키워드 구독자
        List<Long> matchedGlobalIds = n.getKeywords().stream().map(Keyword::getId).toList();
        if (!matchedGlobalIds.isEmpty()) {
            userRepo.findAllBySubscribedKeywords_IdIn(matchedGlobalIds)
                    .forEach(u -> targets.add(u.getId()));
        }

        // 2-b) 개인 키워드 소유자(제목 포함) - dB에서 바로 매칭
        keywordRepo.findOwnerIdsMatchedByTitle(title).forEach(targets::add);

        // 3) 사용자 토픽으로 전송
        for (Long uid : targets) {
            push.sendToUserTopic(uid, type, title, link);
        }
    }
}
