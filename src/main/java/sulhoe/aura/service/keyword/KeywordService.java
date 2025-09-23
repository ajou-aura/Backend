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

    private Set<String> cachedGlobalNorms;
    private void refreshGlobalCache() {
        this.cachedGlobalNorms = keywordRepo.findAllByScope(Scope.GLOBAL).stream()
                .map(Keyword::getPhrase)
                .map(KeywordService::normalizeForCompare)
                .collect(Collectors.toUnmodifiableSet());
    }

    // 부팅 시
    @EventListener(ApplicationReadyEvent.class)
    @Transactional
    public void initOnReady() {
        seedGlobalsIfNeeded();   // 아래로 분리
        refreshGlobalCache();
        if (retagOnStart)
            retagAll();          // 아래로 분리

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

        // 기존 글로벌만 제거
        managed.getKeywords().removeIf(k -> k.getScope() == Scope.GLOBAL);

        // 매칭된 글로벌만 추가
        globals.stream()
                .filter(k -> containsIgnoreCase(title, k.getPhrase()))
                .forEach(managed.getKeywords()::add);
    }

    /* ===== 유틸 ===== */
    private static boolean containsIgnoreCase(String haystack, String needle) {
        if (haystack == null || needle == null) return false;
        String h = normalizeForCompare(haystack);
        String n = normalizeForCompare(needle);
        return !n.isEmpty() && h.contains(n);
    }

    /* ===== 개인 키워드 CRUD ===== */
    @Transactional
    public Keyword addMyKeyword(Long ownerId, String phrase) {
        final String norm = normalizeForCompare(phrase);
        if (norm.isEmpty()) {
            throw new sulhoe.aura.handler.ApiException(
                    org.springframework.http.HttpStatus.BAD_REQUEST,
                    "요청 형식이 올바르지 않습니다.",
                    "VALIDATION_ERROR",
                    "phrase"
            );
        }
        // (A) 전역 키워드 충돌: GLOBAL만 읽어 정규화 후 비교
        if (cachedGlobalNorms == null) {
            refreshGlobalCache(); // 아래 헬퍼
        }
        final boolean conflictsWithGlobal =
                cachedGlobalNorms.contains(norm) // 캐시 히트
                        || keywordRepo.findAllByScope(Scope.GLOBAL).stream() // 극히 드문 캐시 미스 대비
                        .map(Keyword::getPhrase)
                        .map(KeywordService::normalizeForCompare)
                        .anyMatch(norm::equals);

        if (conflictsWithGlobal) {
            // 409 CONFLICT: 전역 키워드와 동일(정규화 기준)
            throw new sulhoe.aura.handler.ApiException(
                    org.springframework.http.HttpStatus.CONFLICT,
                    "기본 키워드와 중복될 수 없습니다.",
                    "CONFLICT_WITH_GLOBAL",
                    "phrase"
            );
        }

        // (B) 개인 키워드 중복: 소유자 보유 키워드를 정규화해 비교(공백/문자폭까지 동일 차단)
        final Optional<Keyword> dup = keywordRepo.findAllByOwnerId(ownerId).stream()
                .filter(k -> normalizeForCompare(k.getPhrase()).equals(norm))
                .findFirst();

        if (dup.isPresent()) {
            throw new sulhoe.aura.handler.ApiException(
                    org.springframework.http.HttpStatus.CONFLICT,
                    "이미 추가된 키워드입니다.",
                    "DUPLICATE_PERSONAL",
                    "phrase"
            );
        }

        // (C) 저장: 표시용 원문은 trim만 권장(보이기 예쁨)
        return keywordRepo.save(Keyword.builder()
                .phrase(phrase == null ? "" : phrase.trim())
                .scope(Scope.USER)
                .ownerId(ownerId)
                .build());
    }

    @Transactional
    public void deleteMyKeyword(Long ownerId, Long keywordId) {
        Keyword k = keywordRepo.findById(keywordId).orElseThrow(() ->
                new sulhoe.aura.handler.ApiException(
                        org.springframework.http.HttpStatus.NOT_FOUND,
                        "대상을 찾을 수 없습니다.",
                        "NOT_FOUND",
                        "id"
                )
        );
        // 1) GLOBAL 삭제 시도 → 409
        if (k.getScope() == Scope.GLOBAL) {
            throw new sulhoe.aura.handler.ApiException(
                    org.springframework.http.HttpStatus.CONFLICT,
                    "기본 키워드는 삭제할 수 없습니다.",
                    "GLOBAL_KEYWORD_DELETE_NOT_ALLOWED",
                    "id"
            );
        }
        // 2) 내 소유가 아니면 존재를 숨기고 404
        if (!Objects.equals(k.getOwnerId(), ownerId)) {
            throw new sulhoe.aura.handler.ApiException(
                    org.springframework.http.HttpStatus.NOT_FOUND,
                    "대상을 찾을 수 없습니다.",
                    "NOT_FOUND",
                    "id"
            );
        }
        // 3) 삭제
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

    @Transactional
    public void subscribePersonal(Long userId, Long personalKeywordId) {
        Keyword k = keywordRepo.findById(personalKeywordId).orElseThrow();
        if (k.getScope() != Scope.USER) {
            throw new sulhoe.aura.handler.ApiException(
                    org.springframework.http.HttpStatus.BAD_REQUEST,
                    "USER 키워드만 구독할 수 있습니다.",
                    "ONLY_USER_ALLOWED",
                    "personalKeywordId"
            );
        }

        User u = userRepo.findById(userId).orElseThrow();
        u.getSubscribedKeywords().add(k);
        userRepo.save(u);
    }

    @Transactional
    public void unsubscribePersonal(Long userId, Long personalKeywordId) {
        User u = userRepo.findById(userId).orElseThrow();
        u.getSubscribedKeywords().removeIf(k -> Objects.equals(k.getId(), personalKeywordId));
        userRepo.save(u);
    }

    @Transactional(readOnly = true)
    public List<Long> myPersonalSubscriptionIds(Long userId) {
        return userRepo.findById(userId).orElseThrow()
                .getSubscribedKeywords().stream()
                .filter(k -> k.getScope() == Scope.USER)
                .map(Keyword::getId)
                .toList();
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

        // 2-c) 개인 키워드 "구독자" (제목 매칭)
        List<Long> matchedPersonalIds = keywordRepo.findAllByScope(Scope.USER).stream()
                .filter(k -> containsIgnoreCase(title, k.getPhrase()))
                .map(Keyword::getId)
                .toList();

        if (!matchedPersonalIds.isEmpty()) {
            userRepo.findAllBySubscribedKeywords_IdIn(matchedPersonalIds)
                    .forEach(u -> targets.add(u.getId()));
        }

        // 3) 사용자 토픽으로 전송
        for (Long uid : targets) {
            push.sendToUserTopic(uid, type, title, link);
        }
    }

    /* ===== 유틸 ===== */
    private static String normalizeForCompare(String s) {
        if (s == null) return "";
        // 1) 트림
        String t = s.trim();
        // 2) 유니코드 정규화(NFKC: 전각/반각, 합성문자 등 통합)
        t = java.text.Normalizer.normalize(t, java.text.Normalizer.Form.NFKC);
        // 3) 소문자(루트 로케일)
        t = t.toLowerCase(java.util.Locale.ROOT);
        // 4) 연속 공백 축약
        t = t.replaceAll("\\s+", " ");
        // 5) 최종 트림
        return t.trim();
    }
}
