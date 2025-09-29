package sulhoe.aura.service.keyword;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import sulhoe.aura.entity.*;
import sulhoe.aura.entity.Keyword.Scope;
import sulhoe.aura.entity.Notice;
import sulhoe.aura.entity.User;
import sulhoe.aura.handler.ApiException;
import sulhoe.aura.repository.KeywordRepository;
import sulhoe.aura.repository.NoticeRepository;
import sulhoe.aura.repository.UserRepository;
import sulhoe.aura.repository.*;
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

    // type 기반
    private final UserTypePreferenceRepository utpRepo;
    private final UserTypeKeywordRepository utikRepo;

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
        Notice managed = noticeRepo.findById(noticeId).orElseThrow(()->
                new ApiException(
                        HttpStatus.NOT_FOUND,
                        "대상을 찾을 수 없습니다.",
                        "NOT_FOUND",
                        "noticeId"
                ));
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

    /* ===== 개인 키워드 CRUD ===== */
    @Transactional
    public Keyword addMyKeyword(Long ownerId, String phrase) {
        final String norm = normalizeForCompare(phrase);
        if (norm.isEmpty()) {
            throw new ApiException(
                    HttpStatus.BAD_REQUEST,
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
            throw new ApiException(
                    HttpStatus.CONFLICT,
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
            throw new ApiException(
                    HttpStatus.CONFLICT,
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
                new ApiException(
                        HttpStatus.NOT_FOUND,
                        "대상을 찾을 수 없습니다.",
                        "NOT_FOUND",
                        "id"
                )
        );
        // 1) GLOBAL 삭제 시도 → 409
        if (k.getScope() == Scope.GLOBAL) {
            throw new ApiException(
                    HttpStatus.CONFLICT,
                    "기본 키워드는 삭제할 수 없습니다.",
                    "GLOBAL_KEYWORD_DELETE_NOT_ALLOWED",
                    "id"
            );
        }
        // 2) 내 소유가 아니면 존재를 숨기고 404
        if (!Objects.equals(k.getOwnerId(), ownerId)) {
            throw new ApiException(
                    HttpStatus.NOT_FOUND,
                    "대상을 찾을 수 없습니다.",
                    "NOT_FOUND",
                    "id"
            );
        }
        // 3) 삭제
        keywordRepo.delete(k);
    }

    // 전역 + 개인 키워드 리스트
    @Transactional(readOnly = true)
    public List<Keyword> listAllForUser(Long ownerId) {
        List<Keyword> res = new ArrayList<>();
        res.addAll(keywordRepo.findAllByScope(Scope.GLOBAL));
        res.addAll(keywordRepo.findAllByOwnerId(ownerId));
        return res;
    }

    // 전역 키워드 리스트
    @Transactional(readOnly = true)
    public List<Keyword> listGlobals() {
        return keywordRepo.findAllByScope(Scope.GLOBAL);
    }

    // 개인 키워드 리스트
    @Transactional(readOnly = true)
    public List<Keyword> listPersonalForUser(Long ownerId) {
        return keywordRepo.findAllByOwnerId(ownerId);
    }

    /* ====== type 기반 구독/키워드 연결 ====== */
  
    @Transactional
    public void setTypeMode(Long userId, String type, UserTypePreference.Mode mode) {
        User u = userRepo.findById(userId).orElseThrow();
        UserTypePreference pref = utpRepo.findByUserIdAndType(userId, type)
                .orElse(UserTypePreference.builder().user(u).type(type).build());
        pref.setMode(mode);
        utpRepo.save(pref);
    }

    /**
     * 유저가 특정 type에 '키워드(전역 또는 개인)'를 구독으로 연결
     * - GLOBAL: 누구나 연결 가능
     * - USER  : 해당 키워드의 ownerId == userId 인 경우에만 연결 가능
     */
    @Transactional
    public Long addUserTypeKeyword(Long userId, String type, Long keywordId) {
        Keyword k = keywordRepo.findById(keywordId).orElseThrow();
        if (k.getScope() == Scope.USER && !Objects.equals(k.getOwnerId(), userId)) {
            throw new sulhoe.aura.handler.ApiException(
                    org.springframework.http.HttpStatus.BAD_REQUEST,
                    "본인 소유의 USER 키워드만 연결할 수 있습니다.",
                    "ONLY_OWN_USER_KEYWORD",
                    "keywordId"
            );
        }
        if (!utikRepo.existsByUser_IdAndTypeAndKeyword_Id(userId, type, keywordId)) {
            User u = userRepo.findById(userId).orElseThrow();
            utikRepo.save(UserTypeKeyword.builder().user(u).type(type).keyword(k).build());
        }
        return keywordId;
    }

    @Transactional
    public void removeUserTypeKeyword(Long userId, String type, Long keywordId) {
        utikRepo.findAllByUserAndType(userId, type).stream()
                .filter(utik -> utik.getKeyword().getId().equals(keywordId))
                .findFirst()
                .ifPresent(utikRepo::delete);
    }

    // 현재 모드 조회
    @Transactional(readOnly = true)
    public UserTypePreference.Mode getTypeMode(Long userId, String type) {
        return utpRepo.findByUserIdAndType(userId, type)
                .map(UserTypePreference::getMode)
                .orElse(null); // 아직 설정 안 했을 수 있음
    }

    // 해당 type에서 내가 구독한 키워드 ID 조회
    @Transactional(readOnly = true)
    public List<Long> listUserTypeKeywordIds(Long userId, String type) {
        return utikRepo.findAllByUserAndType(userId, type)
                .stream()
                .map(utik -> utik.getKeyword().getId())
                .toList();
    }

    /**
     * 저장 직후 호출: (1) 전역 태깅, (2) type 모드별 FCM
     *  - ALL 모드: type 토픽 1회 발송
     *  - KEYWORD 모드: 해당 type에서 '유저가 구독한 여러 키워드(전역/개인)' 중
     *                  어느 하나라도 제목에 매칭되면(OR) 개별 전송
     */
    @Transactional
    public void onNoticeSaved(Notice detachedNotice, String type) {
        Notice n = noticeRepo.findByIdWithKeywords(detachedNotice.getId())
                .orElseThrow(() -> new ApiException(
                        HttpStatus.NOT_FOUND,
                        "대상을 찾을 수 없습니다.",
                        "NOT_FOUND",
                        "noticeId"
                ));

        // 1) 전역 키워드 태깅(검색 캐시 유지)
        final List<Keyword> globals = keywordRepo.findAllByScope(Scope.GLOBAL);
        tagNoticeWithGlobalKeywords(n.getId(), globals);

        final String title = Optional.ofNullable(n.getTitle()).orElse("");
        final String link  = n.getLink();

        // 2) ALL 모드 사용자 존재 시: type 토픽 1회 발송
        List<Long> allModeUserIds = utpRepo.findAllUserIdsByTypeAndAll(type);
        if (!allModeUserIds.isEmpty()) {
            push.sendToTypeTopic(type, title, link);
        }

        // 3) KEYWORD 모드 + 키워드 OR 매칭 사용자
        List<Long> keywordModeUserIds = utpRepo.findAllUserIdsByTypeAndKeyword(type);
        if (keywordModeUserIds.isEmpty()) return;

        // user_type_keywords 의 (user,type,keyword) 중 keyword.phrase 가 제목에 매칭되는 사용자들 (OR)
        List<Long> matchedByTitle = utikRepo.findUserIdsByTypeAndTitleMatch(type, title);

        Set<Long> targets = new LinkedHashSet<>();
        for (Long uid : matchedByTitle) {
            if (keywordModeUserIds.contains(uid)) targets.add(uid);
        }

        for (Long uid : targets) {
            push.sendToUserTopic(uid, type, title, link);
        }

        log.debug("[onNoticeSaved] type={}, ALL={} users, KEYWORD={} users, matchedTargets={}",
                type, allModeUserIds.size(), keywordModeUserIds.size(), targets.size());
    }
}
