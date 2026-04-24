# Security & Traffic Hardening

## TL;DR

> **Quick Summary**: Fix 5 security vulnerabilities (CSRF bypass, SSO ticket exhaustion, admin RBAC, refresh token exposure, OAuth state) and 5 performance bottlenecks (synchronous scraping, N+1 queries, FCM single-send, EAGER fetch, keyword reload) in the Aura backend using a TDD approach.
>
> **Deliverables**:
> - CSRF protection restored for cookie-authenticated endpoints
> - Bounded SSO ticket store with scheduled cleanup
> - Role-based admin access control via Spring Security
> - SHA-256 hashed refresh tokens in DB
> - Cryptographic OAuth state parameter (CSRF nonce + mode)
> - `FORCE_BACKFILL_MISSING` configurable and default-off
> - Batched notice persistence (eliminate redundant `existsByLink`)
> - Batched FCM sends via `sendEach`/`sendMulticast`
> - `User.departments` changed to LAZY with `@Transactional` guards
> - Global keyword cache reused in fanout (no per-notice DB query)
> - Security filter chain integration tests
> - Performance regression tests for scrape and fanout paths
>
> **Estimated Effort**: Large
> **Parallel Execution**: YES - 5 waves
> **Critical Path**: T0 (test infra) → T1-T5 (Wave 1) → T6-T8 (Wave 2) → T9-T11 (Wave 3) → T12-T14 (Wave 4) → T15 (Wave 5) → F1-F4 (Final)

---

## Context

### Original Request
재실행 완료 후 보안/트래픽 취약점 및 개선점 재검토 요청. 코드 기반 전체 분석 완료.

### Interview Summary
**Key Discussions**:
- Test strategy: TDD (Red-Green-Refactor) 선택
- Scope: 전체 10개 항목 (보안 5 + 성능 5)
- 기존 테스트 인프라: AuthControllerTest 1개뿐, standalone MockMvc

**Research Findings**:
- CSRF: `/api/**` 전체 예외 → 쿠키 인증 API 보호 없음
- SSO 티켓: ConcurrentHashMap 무제한 증식, 만료 정리 없음
- Admin: 이메일 화이트리스트 + RuntimeException → 500 응답
- Refresh Token: 평문 JWT 저장, 로그 출력
- OAuth State: CSRF nonce 없이 모드 구분만
- 스크래핑: 동기 블로킹, 40+ URL 순차 처리
- N+1: existsByLink + findByLink + save per notice
- FCM: 개별 전송, 배치 없음
- User.departments: EAGER 로딩 → N+1
- 키워드: fanout마다 전역 키워드 DB 재조회

### Metis Review
**Identified Gaps** (addressed):
- No migration framework exists → use `spring.jpa.hibernate.ddl-auto=update` (app already uses `create`/`update` via `DDL_AUTO` env var)
- Mobile CSRF capability unclear → mobile uses Bearer tokens, exempt from CSRF
- `Notice.link` duplicates risk → verify before adding unique constraint, add dedup in test
- EAGER→LAZY change may break JSON serialization → add `@Transactional` guards and DTO projection where needed
- Admin role: keep minimal (JWT claim + SecurityConfig), not full RBAC system
- Refresh token hashing: SHA-256 (standard, no external dependency)
- CSRF scope: exempt only `/api/auth/app/exchange` (Bearer-only), keep CSRF for all other `/api/**`
- FCM partial failure: collect per-message `BatchResponse` and log failures, don't retry inline

---

## Work Objectives

### Core Objective
Fix all identified security vulnerabilities and performance bottlenecks with regression protection via TDD.

### Concrete Deliverables
- [ ] `SecurityConfig` CSRF restricted to Bearer-only endpoints
- [ ] `SsoTicketService` bounded with scheduled eviction
- [ ] `AdminPushController` protected by `ROLE_ADMIN` via Spring Security
- [ ] `User.refreshToken` stored as SHA-256 hash
- [ ] OAuth `state` parameter includes cryptographic nonce
- [ ] `FORCE_BACKFILL_MISSING` configurable via `app.notice.force-backfill`
- [ ] `NoticePersistenceService` uses batch `findByLinkIn` + `saveAll`
- [ ] `PushNotificationService` uses `sendEach` for batch FCM
- [ ] `User.departments` changed to `FetchType.LAZY` with `@Transactional`
- [ ] `KeywordService.onNoticeSaved` uses `cachedGlobalNorms` instead of DB query

### Definition of Done
- [ ] All new tests pass: `./gradlew test`
- [ ] App boots successfully: `./gradlew bootRun` reaches "Started AuraApplication"
- [ ] CSRF token required for cookie-authenticated POST/PUT/DELETE to `/api/user/**`, `/api/keywords/**`, `/api/admin/push/**`
- [ ] SSO ticket store bounded to 10,000 entries, expired entries cleaned every 60s
- [ ] Admin endpoints return 403 for non-admin users on `/api/admin/push/topic` (not 500)
- [ ] Refresh token in DB is SHA-256 hash, not plaintext
- [ ] OAuth callback rejects invalid `state` nonce
- [ ] Scraping cycle completes without full backfill when `FORCE_BACKFILL_MISSING=false`
- [ ] Notice persistence uses batch operations (verify via log: single `findByLinkIn` per chunk)
- [ ] FCM sends batched (verify via log: `sendEach` call count << user count)
- [ ] `User.departments` loaded lazily (verify: no extra query when departments not accessed)
- [ ] Global keyword cache used in fanout (verify: `findAllByScope(GLOBAL)` called once per cycle, not per notice)

### Must Have
- All 5 security fixes implemented and tested
- All 5 performance improvements implemented and tested
- Security filter chain integration test
- Scrape/fanout performance regression test
- No breaking changes to existing API contracts

### Must NOT Have (Guardrails)
- **No full RBAC system** — only add `ROLE_ADMIN` to JWT and SecurityConfig, no role management UI
- **No DTO layer rewrite** — keep `User.departments` LAZY but use `@Transactional` + Jackson `@JsonIgnore` where needed, not a full DTO mapper
- **No retry/delivery-tracking system for FCM** — log `BatchResponse` failures, don't build a retry queue
- **No migration framework** — use existing `ddl-auto` mechanism, add Flyway/Liquibase only if future work requires it
- **No Redis or external cache** — use in-memory bounded store for SSO tickets, use `cachedGlobalNorms` for keywords
- **No changes to API response shapes** — maintain backward compatibility for all existing endpoints
- **No scope expansion into observability** — no actuator/metrics addition in this plan

---

## Verification Strategy (MANDATORY)

> **ZERO HUMAN INTERVENTION** - ALL verification is agent-executed. No exceptions.

### Test Decision
- **Infrastructure exists**: YES (JUnit 5 + Spring Boot Test in `build.gradle`)
- **Automated tests**: YES (TDD)
- **Framework**: JUnit 5 + Spring Boot Test + MockMvc
- **If TDD**: Each task follows RED (failing test) → GREEN (minimal impl) → REFACTOR

### QA Policy
Every task MUST include agent-executed QA scenarios.
Evidence saved to `.sisyphus/evidence/task-{N}-{scenario-slug}.{ext}`.

- **Security tests**: Use MockMvc with Security filter chain (not standalone) to verify CSRF, auth, 403 responses
- **Performance tests**: Use `@SpringBootTest` with in-memory DB (H2) to verify batch operations, lazy loading
- **API tests**: Use `curl` against running server for endpoint verification

---

## Execution Strategy

### Parallel Execution Waves

```
Wave 0 (Start Immediately - test infrastructure):
└── T0: Test infrastructure setup [quick]

Wave 1 (After T0 - independent security fixes, MAX PARALLEL):
├── T1: Admin RBAC — SecurityConfig + JWT role [deep]
├── T2: Refresh token SHA-256 hashing [quick]
├── T3: OAuth state cryptographic nonce [quick]
├── T4: SSO ticket store bounds + eviction [quick]
└── T5: CSRF scope restriction [deep]

Wave 2 (After Wave 1 - dependent security + performance):
├── T6: FORCE_BACKFILL configurable [quick]
├── T7: User.departments LAZY + @Transactional [unspecified-high]
└── T8: Global keyword cache reuse in fanout [quick]

Wave 3 (After T7 - dependent performance):
├── T9: Notice persistence batch optimization [deep]
└── T10: FCM batch sending [unspecified-high]

Wave 4 (After Wave 3 and T5 - integration):
└── T11: Security integration test suite [deep]

Wave FINAL (After ALL tasks — 4 parallel reviews, then user okay):
├── F1: Plan compliance audit (oracle)
├── F2: Code quality review (unspecified-high)
├── F3: Real manual QA (unspecified-high)
└── F4: Scope fidelity check (deep)
-> Present results -> Get explicit user okay

Critical Path: T0 → T1/T2/T3/T4/T5 → T6/T7/T8 → T9/T10 → T11 → F1-F4 → user okay
Parallel Speedup: ~60% faster than sequential
Max Concurrent: 5 (Wave 1)
```

### Dependency Matrix

| Task | Depends On | Blocks |
|------|-----------|--------|
| T0 | - | T1-T5 |
| T1 | T0 | T11 |
| T2 | T0 | T11 |
| T3 | T0 | - |
| T4 | T0 | - |
| T5 | T0 | T11 |
| T6 | - | - |
| T7 | - | T9 |
| T8 | - | - |
| T9 | T7 | T11 |
| T10 | - | T11 |
| T11 | T1, T2, T5, T9, T10 | F1-F4 |

### Agent Dispatch Summary

- **Wave 0**: T0 → `quick`
- **Wave 1**: T1 → `deep`, T2 → `quick`, T3 → `quick`, T4 → `quick`, T5 → `deep`
- **Wave 2**: T6 → `quick`, T7 → `unspecified-high`, T8 → `quick`
- **Wave 3**: T9 → `deep`, T10 → `unspecified-high`
- **Wave 4**: T11 → `deep`
- **FINAL**: F1 → `oracle`, F2 → `unspecified-high`, F3 → `unspecified-high`, F4 → `deep`

---

## TODOs

- [x] 0. Test Infrastructure Setup

  **What to do**:
  - Add H2 database test dependency to `build.gradle` (`runtimeOnly 'com.h2database:h2'` with test scope)
  - Create `src/test/resources/application-test.properties` with H2 config, test JWT secret, and `DDL_AUTO=create-drop`
  - Create `src/test/java/sulhoe/aura/config/SecurityTestConfig.java` as a base class for security integration tests that loads the full Security filter chain (not standalone MockMvc)
  - Create `src/test/java/sulhoe/aura/config/TestContainersConfig.java` is NOT needed — use H2 in-memory for all tests
  - Write a test that boots the app context and verifies Security filter chain loads: `@SpringBootTest` → `WebApplicationContext` → filter chain contains `jwtAuthenticationFilter`
  - Run `./gradlew test` to confirm existing `AuthControllerTest` still passes with new test infrastructure

  **Must NOT do**:
  - Do not add Testcontainers or Docker-based test infrastructure
  - Do not modify production `application.properties`
  - Do not change existing `AuthControllerTest` from standalone MockMvc

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: NO (foundation for all other tasks)
  - **Parallel Group**: Wave 0
  - **Blocks**: T1, T2, T3, T4, T5
  - **Blocked By**: None (can start immediately)

  **References**:

  **Pattern References** (existing code to follow):
  - `src/test/java/sulhoe/aura/controller/AuthControllerTest.java` — existing test structure and MockMvc patterns
  - `src/main/java/sulhoe/aura/config/SecurityConfig.java` — security filter chain configuration to load in tests
  - `build.gradle` — current dependency structure, Spring Boot version, test dependencies

  **API/Type References**:
  - `src/main/resources/application.properties` — current config keys for JWT, CORS, datasource (use as reference for test overrides)
  - `src/main/resources/application.yml` — retag-on-start and keyword seed config

  **Test References**:
  - `src/test/java/sulhoe/aura/controller/AuthControllerTest.java` — existing test, verifies AuthController with standalone MockMvc

  **External References**:
  - Spring Boot Testing docs: `https://docs.spring.io/spring-boot/docs/3.4.5/reference/html/features.html#features.testing`

  **WHY Each Reference Matters**:
  - AuthControllerTest shows the existing test style; new tests should use `@SpringBootTest` + `MockMvc` with full context, not standalone
  - SecurityConfig is what we're testing against — must load it, not mock it
  - application.properties keys affect test config overrides (jwt.key, datasource URLs)

  **Acceptance Criteria**:

  **If TDD**:
  - [ ] Test file created: `src/test/java/sulhoe/aura/config/SecurityTestConfig.java`
  - [ ] Test properties created: `src/test/resources/application-test.properties`
  - [ ] `./gradlew test` → ALL tests pass (existing + new)
  - [ ] New test verifies Security filter chain loads with `jwtAuthenticationFilter`

  **QA Scenarios (MANDATORY)**:

  ```
  Scenario: Test infrastructure loads full Spring context
    Tool: Bash
    Preconditions: H2 dependency added, test properties configured
    Steps:
      1. Run `./gradlew test --tests "sulhoe.aura.config.SecurityTestConfig"`
      2. Assert exit code 0
      3. Run `./gradlew test` (full suite)
      4. Assert exit code 0 and all existing tests still pass
    Expected Result: All tests pass, no regression
    Failure Indicators: Context load failure, missing H2 dependency, conflicting test properties
    Evidence: .sisyphus/evidence/task-0-test-infra.txt

  Scenario: H2 in-memory DB works for entity creation
    Tool: Bash
    Preconditions: Test properties use `jdbc:h2:mem:testdb`
    Steps:
      1. Run `./gradlew test --tests "sulhoe.aura.config.SecurityTestConfig.contextLoads"`
      2. Assert test passes
    Expected Result: Application context loads with H2, JPA entities initialize
    Failure Indicators: "Unable to create DataSource", dialect mismatch, H2 not found
    Evidence: .sisyphus/evidence/task-0-h2-context.txt
  ```

  **Commit**: YES
  - Message: `test(infra): add H2 test dependency and security test base config`
  - Files: `build.gradle`, `src/test/resources/application-test.properties`, `src/test/java/sulhoe/aura/config/SecurityTestConfig.java`
  - Pre-commit: `./gradlew test`

- [x] 1. Admin RBAC — SecurityConfig + JWT Role

  **What to do**:
  - RED: Write integration test that sends `POST /api/admin/push/topic` with a regular user JWT → expects 403. Write test with admin email JWT → expects 200 (or 404 if no targets, not 500).
  - GREEN: Add `role` field to `User` entity (enum: `USER`, `ADMIN`) with default `USER`. Update `JwtTokenProvider` to include `role` claim. Update `JwtAuthenticationFilter` to set `ROLE_ADMIN` or `ROLE_USER` authority based on JWT claim. Add `.requestMatchers("/api/admin/**").hasRole("ADMIN")` to `SecurityConfig`. Update `AdminPushController` to remove `ensureAdmin()` method and `RuntimeException`.
  - REFACTOR: Extract role checking into `SecurityConfig` only. Clean up admin email config property if no longer needed.
  - Seed admin user role: add config property `app.admin.emails` for initial admin designation during login.

  **Must NOT do**:
  - Do not build a full RBAC system with multiple roles, role management UI, or dynamic role assignment
  - Do not add a `roles` table — use a simple `role` column on `User`
  - Do not change existing API response shapes for non-admin endpoints

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []
  - **Skills Evaluated but Omitted**:
    - `refactor-safely`: Not needed — this is new feature addition, not refactoring

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T2, T3, T4)
  - **Parallel Group**: Wave 1
  - **Blocks**: T11
  - **Blocked By**: T0

  **References**:

  **Pattern References**:
  - `src/main/java/sulhoe/aura/config/SecurityConfig.java:70-85` — current `permitAll` and `authenticated` rules, where `.hasRole("ADMIN")` must be added
  - `src/main/java/sulhoe/aura/controller/AdminPushController.java:33-57` — current email whitelist logic to remove
  - `src/main/java/sulhoe/aura/config/JwtAuthenticationFilter.java:44-86` — filter where role authority is set (currently hardcoded `ROLE_USER`)

  **API/Type References**:
  - `src/main/java/sulhoe/aura/entity/User.java` — entity where `role` field must be added
  - `src/main/java/sulhoe/aura/config/JwtTokenProvider.java` — token creation/validation where `role` claim must be added
  - `src/main/java/sulhoe/aura/service/login/AuthService.java` — login flow where admin designation will be applied

  **WHY Each Reference Matters**:
  - SecurityConfig is the central authorization point — `.hasRole("ADMIN")` goes there
  - AdminPushController is where the email whitelist check currently lives — it must be removed
  - JwtAuthenticationFilter hardcodes `ROLE_USER` — must read role from JWT claim instead
  - JwtTokenProvider must emit the role claim during token creation

  **Acceptance Criteria**:

  **If TDD**:
  - [ ] Test: regular user JWT → `POST /api/admin/push/topic` → 403
  - [ ] Test: admin JWT → `POST /api/admin/push/topic` → 200 or 404 (not 500)
  - [ ] `./gradlew test` → ALL tests pass

  **QA Scenarios (MANDATORY)**:

  ```
  Scenario: Regular user cannot access admin endpoints
    Tool: MockMvc (Spring Boot integration test)
    Preconditions: Full app context loaded with H2, regular user JWT created via JwtTokenProvider
    Steps:
      1. Create a JWT with ROLE_USER authority using test helper
      2. Perform POST /api/admin/push/topic with the regular user JWT as Bearer token
      3. Assert HTTP 403 with FORBIDDEN code
    Expected Result: 403 Forbidden with FORBIDDEN code
    Failure Indicators: 200 OK (RBAC not enforced), 500 (RuntimeException instead of proper 403)
    Evidence: .sisyphus/evidence/task-1-admin-403.txt

  Scenario: Admin user can access admin endpoints
    Tool: MockMvc (Spring Boot integration test)
    Preconditions: Full app context loaded with H2, admin user JWT created via JwtTokenProvider
    Steps:
      1. Create a JWT with ROLE_ADMIN authority using test helper
      2. Perform POST /api/admin/push/topic with admin JWT as Bearer token
      3. Assert HTTP is not 403 and not 500
    Expected Result: 200 or 404 (valid response, not access denied)
    Failure Indicators: 403 (role claim not in JWT), 500 (RuntimeException still thrown)
    Evidence: .sisyphus/evidence/task-1-admin-access.txt
  ```

  **Commit**: YES (groups with T1)
  - Message: `feat(security): add ROLE_ADMIN to JWT and protect admin endpoints`
  - Files: `SecurityConfig.java`, `JwtTokenProvider.java`, `JwtAuthenticationFilter.java`, `AdminPushController.java`, `User.java`, `AuthService.java`, test files
  - Pre-commit: `./gradlew test`

- [x] 2. Refresh Token SHA-256 Hashing

  **What to do**:
  - RED: Write test that verifies `User.refreshToken` does NOT contain the raw JWT value after a refresh cycle.
  - GREEN: Add `hashSha256()` utility method in `AuthService`. In `createRefreshToken()`, store `SHA-256(refreshToken)` instead of the raw token. In `rotateRefreshTokenAtomically()`, hash the incoming token before comparing. In `AuthService.refresh()`, hash before lookup and comparison.
  - REFACTOR: Consider extracting a `TokenHasher` utility, but keep it simple — a static method is fine.

  **Must NOT do**:
  - Do not add a separate `refresh_token_hash` column — reuse the existing `refreshToken` column
  - Do not change the API contract — clients still send raw tokens, the DB just stores hashes
  - Do not add bcrypt or argon2 — SHA-256 is sufficient for non-password tokens

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T1, T3, T4)
  - **Parallel Group**: Wave 1
  - **Blocks**: T11
  - **Blocked By**: T0

  **References**:

  **Pattern References**:
  - `src/main/java/sulhoe/aura/service/login/AuthService.java:54-64,93-114` — where refresh tokens are created and compared
  - `src/main/java/sulhoe/aura/repository/UserRepository.java:12-18` — `rotateRefreshTokenAtomically` JPQL query

  **API/Type References**:
  - `src/main/java/sulhoe/aura/entity/User.java:29` — `refreshToken` field that stores the hash

  **Test References**:
  - `src/test/java/sulhoe/aura/controller/AuthControllerTest.java` — existing auth test patterns

  **WHY Each Reference Matters**:
  - AuthService is where token creation and comparison happen — hashing must be added at both points
  - UserRepository JPQL query compares raw tokens — must compare hashes instead
  - User entity column semantics change from "raw token" to "token hash"

  **Acceptance Criteria**:

  **If TDD**:
  - [ ] Test: after refresh, `User.refreshToken` in DB ≠ raw JWT value
  - [ ] Test: refresh with correct token succeeds, wrong token fails
  - [ ] `./gradlew test` → ALL tests pass

  **QA Scenarios (MANDATORY)**:

  ```
Scenario: Refresh token stored as hash, not plaintext
    Tool: MockMvc (Spring Boot integration test)
    Preconditions: Full app context with H2
    Steps:
      1. Create user via repository, then call AuthService.createRefreshToken()
      2. Query UserRepository for the stored refreshToken value
      3. Assert stored value ≠ raw refresh token value
      4. Call AuthService.refresh() with the raw token → success
    Expected Result: DB stores SHA-256 hash, not raw token; refresh still works
    Failure Indicators: DB value matches raw token (hashing not applied), refresh fails (comparison broken)
    Evidence: .sisyphus/evidence/task-2-hash-stored.txt

  Scenario: Stale refresh token rejected
    Tool: MockMvc (Spring Boot integration test)
    Preconditions: Full app context with H2
    Steps:
      1. Create user, create refresh token R1
      2. Call refresh with R1 → success, get R2
      3. Call refresh with R1 again → 401
    Expected Result: First refresh succeeds, second fails (token already rotated)
    Failure Indicators: R1 accepted after rotation (hash collision or comparison bug)
    Evidence: .sisyphus/evidence/task-2-stale-rejected.txt
  ```

  **Commit**: YES
  - Message: `feat(security): hash refresh tokens with SHA-256 before DB storage`
  - Files: `AuthService.java`, `UserRepository.java`, `User.java`, test files
  - Pre-commit: `./gradlew test`

- [x] 3. OAuth State Cryptographic Nonce

  **What to do**:
  - RED: Write test that verifies `/api/auth/callback` rejects requests with unknown or expired `state` parameter.
  - GREEN: In `AuthController.redirectToGoogle()`, generate `state = Base64(nonce:mode)` where `nonce` is a 32-byte cryptographic random string. Store `nonce → (mode, expiry)` in `SsoTicketService` (or a separate `StateNonceStore`). In `AuthController.callback()`, validate `nonce` exists and hasn't expired before extracting mode. Remove unused direct `mode` usage from callback.
  - REFACTOR: Keep the `SsoTicketService` store pattern but add a separate `stateNonceStore` map or reuse the same store with a different prefix.

  **Must NOT do**:
  - Do not build a full OAuth PKCE flow — just add nonce to state
  - Do not change the Google OAuth redirect URI format
  - Do not break the `state=app` deep-link flow — keep the app/web branching behavior

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T1, T2, T4)
  - **Parallel Group**: Wave 1
  - **Blocks**: None
  - **Blocked By**: T0

  **References**:

  **Pattern References**:
  - `src/main/java/sulhoe/aura/controller/AuthController.java:120-133,137-186` — where `state` is set and read
  - `src/main/java/sulhoe/aura/service/login/SsoTicketService.java:8-29` — in-memory store pattern to follow for nonce storage

  **API/Type References**:
  - `docs/react-native-auth.md` — documents the app auth flow, must be updated if state format changes

  **Test References**:
  - `src/test/java/sulhoe/aura/controller/AuthControllerTest.java` — existing auth test patterns

  **WHY Each Reference Matters**:
  - AuthController is where state is created (redirectToGoogle) and consumed (callback)
  - SsoTicketService provides the pattern for short-lived in-memory stores with TTL — reuse this pattern
  - React-native-auth doc must stay consistent with the actual state format

  **Acceptance Criteria**:

  **If TDD**:
  - [ ] Test: callback with invalid/expired state → 401
  - [ ] Test: callback with valid state → success (app or web flow)
  - [ ] `./gradlew test` → ALL tests pass

  **QA Scenarios (MANDATORY)**:

  ```
  Scenario: Invalid state parameter rejected
    Tool: MockMvc (Spring Boot integration test)
    Preconditions: Full app context with Security filter chain
    Steps:
      1. Perform GET /api/auth/callback?state=invalidnonce&code=xxx
      2. Assert HTTP 401
    Expected Result: 401 Unauthorized
    Failure Indicators: 200 (state not validated), redirect to frontend (callback accepted without validation)
    Evidence: .sisyphus/evidence/task-3-invalid-state.txt

  Scenario: Valid state parameter accepted
    Tool: MockMvc (Spring Boot integration test)
    Preconditions: Full app context with StateNonceStore
    Steps:
      1. Generate a valid state nonce via SsoTicketService or StateNonceStore
      2. Perform GET /api/auth/callback?state=<valid-nonce>&code=xxx (or MockMvc equivalent)
      3. Assert callback processes without 401 (even if downstream OAuth fails, state validation passes)
    Expected Result: State validation passes, no 401 for invalid state
    Failure Indicators: 401 on valid state, wrong mode extraction
    Evidence: .sisyphus/evidence/task-3-valid-state.txt
  ```

  **Commit**: YES
  - Message: `feat(security): add cryptographic nonce to OAuth state parameter`
  - Files: `AuthController.java`, `SsoTicketService.java` (or new `StateNonceStore.java`), `AuthControllerTest.java`
  - Pre-commit: `./gradlew test`

- [x] 4. SSO Ticket Store Bounds + Eviction

  **What to do**:
  - RED: Write test that creates 10,001 tickets and verifies store rejects after 10,000. Write test that expired tickets are evicted by the cleanup scheduler.
  - GREEN: Add a `maxSize` cap (10,000) to `ConcurrentHashMap` — reject new ticket issuance when `store.size() >= maxSize`. Add `@Scheduled(fixedDelay = 60_000)` cleanup method that removes entries where `exp < System.currentTimeMillis()`.
  - REFACTOR: Consider extracting `SsoTicketStore` interface if future externalization is planned.

  **Must NOT do**:
  - Do not add Redis or external cache — stay in-memory
  - Do not change the ticket exchange API contract
  - Do not change the 2-minute TTL duration

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T1, T2, T3)
  - **Parallel Group**: Wave 1
  - **Blocks**: None
  - **Blocked By**: T0

  **References**:

  **Pattern References**:
  - `src/main/java/sulhoe/aura/service/login/SsoTicketService.java:8-29` — current unbounded ConcurrentHashMap store
  - `src/main/java/sulhoe/aura/service/notice/ScrapeScheduleService.java:21-50` — example of `@Scheduled` pattern in existing code

  **API/Type References**:
  - `AuthController.java:225-238` — where `SsoTicketService.create()` is called

  **Test References**:
  - `src/test/java/sulhoe/aura/controller/AuthControllerTest.java:45-63,66-133` — existing SSO ticket exchange tests

  **WHY Each Reference Matters**:
  - SsoTicketService is the file being modified — currently unbounded, needs cap + eviction
  - ScrapeScheduleService shows the `@Scheduled` annotation pattern used in this project
  - AuthController tests cover ticket exchange behavior — must still pass after bounding

  **Acceptance Criteria**:

  **If TDD**:
  - [ ] Test: store rejects 10,001st ticket when max is 10,000
  - [ ] Test: expired tickets removed by cleanup
  - [ ] `./gradlew test` → ALL tests pass

  **QA Scenarios (MANDATORY)**:

  ```
  Scenario: Ticket store bound enforcement
    Tool: JUnit test (MockMvc or unit test)
    Preconditions: Clean store, max size 10000
    Steps:
      1. Create 10000 tickets via SsoTicketService.create()
      2. Attempt to create ticket 10001
      3. Assert rejection (ApiException or similar)
    Expected Result: 10001st ticket creation fails gracefully
    Failure Indicators: No cap enforced (OOM risk), exception crashes app
    Evidence: .sisyphus/evidence/task-4-store-bound.txt

  Scenario: Expired tickets evicted
    Tool: JUnit test
    Preconditions: Store has tickets with past expiry
    Steps:
      1. Create ticket with 1ms TTL
      2. Wait 10ms for expiry
      3. Call SsoTicketService.cleanupExpired() (or wait for @Scheduled)
      4. Assert ticket not in store
    Expected Result: Expired tickets removed from store
    Failure Indicators: Expired tickets remain in store (memory leak)
    Evidence: .sisyphus/evidence/task-4-expiry-eviction.txt
  ```

  **Commit**: YES
  - Message: `fix(security): bound SSO ticket store and add scheduled eviction`
  - Files: `SsoTicketService.java`, test files
  - Pre-commit: `./gradlew test`

- [x] 5. CSRF Scope Restriction

  **What to do**:
  - RED: Write test that sends `POST /api/user/departments` with cookie auth but no CSRF token → expects 403. Write test that sends `POST /api/auth/app/exchange` with Bearer token → expects 200 (no CSRF needed).
  - GREEN: In `SecurityConfig`, replace `.ignoringRequestMatchers(new MvcRequestMatcher(introspector(), "/api/**"))` with `.ignoringRequestMatchers(new MvcRequestMatcher(introspector(), "/api/auth/app/exchange"))`. Also exempt `GET` requests and `OPTIONS` (already done). The CSRF token endpoint `/api/auth/csrf` must still work.
  - REFACTOR: Audit all POST/PUT/DELETE endpoints that use cookie auth to confirm they'll receive CSRF tokens.
  - Verify mobile app flow (Bearer token) is not affected.

  **Must NOT do**:
  - Do not add CSRF protection to Bearer-only endpoints (`/api/auth/app/exchange`)
  - Do not remove the `/api/auth/csrf` endpoint
  - Do not change the `SameSite=None` cookie setting (needed for cross-origin mobile/web)

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T1, T2, T3, T4)
  - **Parallel Group**: Wave 1
  - **Blocks**: T11
  - **Blocked By**: T0

  **References**:

  **Pattern References**:
  - `src/main/java/sulhoe/aura/config/SecurityConfig.java:41-62` — current CSRF config with blanket `/api/**` exemption
  - `src/main/java/sulhoe/aura/controller/AuthController.java:51-58` — CSRF token endpoint
  - `src/main/java/sulhoe/aura/config/WebConfig.java:23-40` — CORS config that enables credentials

  **API/Type References**:
  - `src/main/java/sulhoe/aura/config/JwtAuthenticationFilter.java:88-115` — token resolution (cookie-first, Bearer second)

  **Test References**:
  - `src/test/java/sulhoe/aura/controller/AuthControllerTest.java` — existing auth test, uses standalone MockMvc (won't test CSRF)

  **WHY Each Reference Matters**:
  - SecurityConfig is the file being modified — replacing `/api/**` exemption with narrow `/api/auth/app/exchange`
  - AuthController exposes the CSRF token endpoint — must still work after change
  - WebConfig CORS + SameSite=None cookies are why CSRF matters — cross-origin cookie-submitted requests need protection
  - Existing AuthControllerTest uses standalone MockMvc which bypasses SecurityConfig — new tests MUST use full context MockMvc

  **Acceptance Criteria**:

  **If TDD**:
  - [ ] Test: POST to protected endpoint with cookie but no CSRF → 403
  - [ ] Test: POST to protected endpoint with cookie + CSRF token → 200
  - [ ] Test: POST to `/api/auth/app/exchange` with Bearer → 200 (no CSRF needed)
  - [ ] `./gradlew test` → ALL tests pass

  **QA Scenarios (MANDATORY)**:

  ```
  Scenario: Cookie-auth POST without CSRF token is rejected
    Tool: MockMvc (Spring Boot integration test)
    Preconditions: Full app context with Security filter chain, user authenticated via cookie
    Steps:
      1. Authenticate user, obtain WEB_SESSION cookie and CSRF token from /api/auth/csrf
      2. Perform POST /api/user/departments with WEB_SESSION cookie but WITHOUT X-CSRF-TOKEN header
      3. Assert HTTP 403 with body containing "CSRF_FAILED"
    Expected Result: 403 Forbidden with CSRF_FAILED code
    Failure Indicators: 200 (CSRF not enforced), different error code
    Evidence: .sisyphus/evidence/task-5-csrf-reject.txt

  Scenario: Bearer-auth POST to app exchange succeeds without CSRF
    Tool: MockMvc (Spring Boot integration test)
    Preconditions: Full app context, valid SSO ticket obtained
    Steps:
      1. Create SSO ticket via SsoTicketService.create()
      2. Perform POST /api/auth/app/exchange with JSON body {"code":"<ticket>"} (no CSRF token)
      3. Assert HTTP 200 (no CSRF required for Bearer-only endpoint)
    Expected Result: 200 OK with access/refresh tokens
    Failure Indicators: 403 (CSRF enforced on Bearer endpoint — must be exempt)
    Evidence: .sisyphus/evidence/task-5-bearer-exempt.txt
  ```

  **Commit**: YES
  - Message: `fix(security): restrict CSRF exemption to Bearer-only endpoints`
  - Files: `SecurityConfig.java`, test files
  - Pre-commit: `./gradlew test`

- [ ] 6. FORCE_BACKFILL Configurable

  **What to do**:
  - RED: Write test that verifies when `app.notice.force-backfill=false`, the scraper uses incremental mode (not full load).
  - GREEN: Replace `FORCE_BACKFILL_MISSING = true` with `@Value("${app.notice.force-backfill:false}") private boolean forceBackfill;`. Update `NoticeScrapeService.isFullLoad()` (or equivalent heuristic) to check `forceBackfill`. Default to `false` in `application.properties`.
  - REFACTOR: Ensure the property name is consistent with existing `app.*` namespace (see `app.keywords.seed` in `application.yml`).

  **Must NOT do**:
  - Do not change the existing incremental/full-load heuristic logic — just make it configurable
  - Do not remove the backfill capability entirely — some environments may need it

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T7, T8)
  - **Parallel Group**: Wave 2
  - **Blocks**: None
  - **Blocked By**: None (independent of T0-T5)

  **References**:

  **Pattern References**:
  - `src/main/java/sulhoe/aura/service/notice/NoticeScrapeService.java:38-39,62-68` — `FORCE_BACKFILL_MISSING = true` hardcode to replace
  - `src/main/resources/application.yml:1-4` — `app.keywords.seed` and `app.keywords.retag-on-start` show `app.*` property naming pattern

  **API/Type References**:
  - `src/main/resources/application.properties` — where the new property default should be added

  **WHY Each Reference Matters**:
  - NoticeScrapeService is where the hardcoded `true` lives — must be externalized
  - application.yml shows the existing `app.*` config namespace convention

  **Acceptance Criteria**:

  **If TDD**:
  - [ ] Test: when `forceBackfill=false`, incremental mode is used
  - [ ] Test: when `forceBackfill=true`, full load mode is used
  - [ ] `./gradlew test` → ALL tests pass

  **QA Scenarios (MANDATORY)**:

  ```
  Scenario: Default backfill is incremental
    Tool: Bash
    Preconditions: `app.notice.force-backfill` not set (defaults to false)
    Steps:
      1. Start app with default config
      2. Trigger scrape (or inspect NoticeScrapeService field value)
      3. Assert fullLoad/finalLoad is NOT forced
    Expected Result: Scraper uses incremental heuristic
    Failure Indicators: Scraper always does full scan
    Evidence: .sisyphus/evidence/task-6-incremental.txt

  Scenario: Backfill enabled via config
    Tool: Bash
    Preconditions: `app.notice.force-backfill=true` set
    Steps:
      1. Start app with `FORCE_BACKFILL=true` env var
      2. Inspect NoticeScrapeService behavior
      3. Assert full load mode is active
    Expected Result: Scraper does full scan when enabled
    Failure Indicators: Property is ignored, always incremental
    Evidence: .sisyphus/evidence/task-6-backfill-enabled.txt
  ```

  **Commit**: YES
  - Message: `refactor(scrape): make backfill configurable via app.notice.force-backfill`
  - Files: `NoticeScrapeService.java`, `application.properties`, test files
  - Pre-commit: `./gradlew test`

- [ ] 7. User.departments LAZY + @Transactional

  **What to do**:
  - RED: Write test that accesses `user.getDepartments()` outside a transaction → expects `LazyInitializationException`. Write test that accesses it inside `@Transactional` → succeeds.
  - GREEN: Change `User.departments` from `FetchType.EAGER` to `FetchType.LAZY`. Add `@Transactional(readOnly = true)` to controller methods and service methods that access `user.getDepartments()`. In `UserController.getUserInfoByEmail()`, either use `@Transactional` or load departments explicitly.
  - REFACTOR: If JSON serialization of `User` entities causes issues, add `@JsonIgnore` on `departments` field in the `User` entity and create a separate accessor in the service layer.

  **Must NOT do**:
  - Do not create a full DTO layer — only add `@Transactional` or `@JsonIgnore` where needed
  - Do not change API response shapes — existing endpoints must return the same JSON structure
  - Do not add a separate `departments` endpoint unless it's needed for the existing mobile app

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T6, T8)
  - **Parallel Group**: Wave 2
  - **Blocks**: T9
  - **Blocked By**: None (independent of T0-T5)

  **References**:

  **Pattern References**:
  - `src/main/java/sulhoe/aura/entity/User.java:31-37` — current `@ElementCollection(fetch = FetchType.EAGER)` declaration
  - `src/main/java/sulhoe/aura/controller/UserController.java` — controller that accesses `user.getDepartments()`
  - `src/main/java/sulhoe/aura/service/keyword/KeywordService.java:401` — `userRepo.findAllById(targets)` where departments are loaded

  **API/Type References**:
  - `src/main/java/sulhoe/aura/repository/UserRepository.java` — user repository queries

  **WHY Each Reference Matters**:
  - User entity has the EAGER fetch type to change
  - UserController and KeywordService access departments — need `@Transactional` or explicit loading
  - Changing to LAZY may break JSON serialization if User entities are returned directly from controllers

  **Acceptance Criteria**:

  **If TDD**:
  - [ ] Test: LAZY departments cause `LazyInitializationException` outside transaction
  - [ ] Test: LAZY departments load successfully inside `@Transactional`
  - [ ] Test: `/api/user/info` response still includes departments (no API shape change)
  - [ ] `./gradlew test` → ALL tests pass

  **QA Scenarios (MANDATORY)**:

  ```
  Scenario: Departments not loaded outside transaction
    Tool: Bash (test)
    Preconditions: User.departments changed to LAZY
    Steps:
      1. Load User entity outside @Transactional context
      2. Call user.getDepartments()
      3. Assert LazyInitializationException thrown
    Expected Result: Exception thrown when accessing LAZY collection outside session
    Failure Indicators: No exception (still EAGER), wrong exception type
    Evidence: .sisyphus/evidence/task-7-lazy-exception.txt

  Scenario: API response includes departments after LAZY change
    Tool: MockMvc (Spring Boot integration test)
    Preconditions: Full app context, user with departments authenticated
    Steps:
      1. Create user with departments in test DB
      2. Authenticate and obtain JWT
      3. Perform GET /api/user/info with Authorization Bearer header
      4. Assert response JSON contains "departments" array
    Expected Result: Same JSON shape as before LAZY change
    Failure Indicators: Missing "departments" field, LazyInitializationException in logs
    Evidence: .sisyphus/evidence/task-7-api-unchanged.txt
  ```

  **Commit**: YES
  - Message: `perf(entity): change User.departments to LAZY fetch with @Transactional`
  - Files: `User.java`, `UserController.java`, `KeywordService.java`, test files
  - Pre-commit: `./gradlew test`

- [ ] 8. Global Keyword Cache Reuse in Fanout

  **What to do**:
  - RED: Write test that verifies `keywordRepo.findAllByScope(GLOBAL)` is called at most once per `onNoticeSaved` cycle (or per notice chunk), not once per notice.
  - GREEN: In `KeywordService.onNoticeSaved()`, use `cachedGlobalNorms` (or a refreshed version) instead of calling `keywordRepo.findAllByScope(GLOBAL)`. Ensure `refreshGlobalCache()` is called at the start of a fanout cycle, not per notice. The cache is already maintained by `seedGlobalsIfNeeded()` and `addMyKeyword()`.
  - REFACTOR: Consider making `cachedGlobalNorms` a `volatile` field with `synchronized` refresh, or use `@Cacheable` if Spring Cache is available.

  **Must NOT do**:
  - Do not add Redis or external cache — use the existing `cachedGlobalNorms` in-memory cache
  - Do not change the keyword matching algorithm
  - Do not change the fanout trigger mechanism

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T6, T7)
  - **Parallel Group**: Wave 2
  - **Blocks**: None
  - **Blocked By**: None (independent of T0-T5)

  **References**:

  **Pattern References**:
  - `src/main/java/sulhoe/aura/service/keyword/KeywordService.java:363-412` — `onNoticeSaved()` method that calls `keywordRepo.findAllByScope(GLOBAL)` per notice
  - `src/main/java/sulhoe/aura/service/keyword/KeywordService.java:47-53` — `cachedGlobalNorms` field and `refreshGlobalCache()` method already exist

  **API/Type References**:
  - `src/main/java/sulhoe/aura/entity/Keyword.java` — `Scope.GLOBAL` enum and `phrase` field

  **WHY Each Reference Matters**:
  - onNoticeSaved is the hot path called per notice — currently does a DB query for globals each time
  - cachedGlobalNorms is already maintained but not used in the fanout path
  - refreshGlobalCache() already exists — just needs to be called at the right time

  **Acceptance Criteria**:

  **If TDD**:
  - [ ] Test: `findAllByScope(GLOBAL)` is called at most once per fanout cycle, not per notice
  - [ ] Test: keyword matching still works correctly with cached data
  - [ ] `./gradlew test` → ALL tests pass

  **QA Scenarios (MANDATORY)**:

  ```
  Scenario: Global keywords loaded once per cycle, not per notice
    Tool: Bash (test with mock repository)
    Preconditions: Multiple notices saved in one cycle
    Steps:
      1. Set up mock KeywordRepository with findAllByScope returning test data
      2. Save 5 notices in sequence via onNoticeSaved
      3. Count findAllByScope invocations
      4. Assert invocations = 1 (or 0 if cache pre-loaded), not 5
    Expected Result: DB query for global keywords called at most once
    Failure Indicators: 5 or more calls (cache not used)
    Evidence: .sisyphus/evidence/task-8-cache-reuse.txt

  Scenario: Keyword matching still accurate with cached data
    Tool: Bash (test)
    Preconditions: Global keywords seeded
    Steps:
      1. Save a notice with title matching a global keyword
      2. Assert notice gets tagged with that keyword
    Expected Result: Notice correctly tagged via cache
    Failure Indicators: Notice not tagged, or wrong tags applied
    Evidence: .sisyphus/evidence/task-8-matching-accurate.txt
  ```

  **Commit**: YES
  - Message: `perf(keyword): reuse global keyword cache in fanout path`
  - Files: `KeywordService.java`, test files
  - Pre-commit: `./gradlew test`

- [ ] 9. Notice Persistence Batch Optimization

  **What to do**:
  - RED: Write test that verifies `findByLinkIn` is called once per chunk (not `findByLink` per notice). Write test that `existsByLink` is no longer called before `saveOrUpdateOne`.
  - GREEN: In `NoticeScrapeService`, replace the per-notice `existsByLink()` check with a batch pre-check: collect all links in a chunk, call `repo.findByLinkIn(links)` once, then only process new notices. In `NoticePersistenceService`, keep `saveOrUpdateOne` but remove the redundant `existsByLink` call in `NoticeScrapeService`. Consider using `saveAll` for batch inserts where new notices are identified.
  - REFACTOR: The `saveOrUpdateOne` method can stay for individual saves, but new-notice creation should prefer `saveAll`.

  **Must NOT do**:
  - Do not change the `REQUIRES_NEW` transaction propagation yet — change would be too risky without performance profiling
  - Do not add Flyway migration for `Notice.link` unique constraint in this task (that's a DBA concern)
  - Do not change the scraping schedule or parser logic

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T10)
  - **Parallel Group**: Wave 3
  - **Blocks**: T11
  - **Blocked By**: T7 (LAZY departments change affects user queries that may be in the same path)

  **References**:

  **Pattern References**:
  - `src/main/java/sulhoe/aura/service/notice/NoticeScrapeService.java:86-99,152-171` — where `existsByLink` and `scrapeNotices` iterate per notice
  - `src/main/java/sulhoe/aura/service/notice/NoticePersistenceService.java:20-49` — `saveOrUpdateOne` with `findByLink` + `save`
  - `src/main/java/sulhoe/aura/repository/NoticeRepository.java:15-22` — `existsByLink` and `findByLink` methods

  **API/Type References**:
  - `src/main/java/sulhoe/aura/entity/Notice.java:21-48` — Notice entity with `link` field

  **WHY Each Reference Matters**:
  - NoticeScrapeService has the redundant `existsByLink` calls to remove
  - NoticePersistenceService has the `findByLink` + `save` pattern to optimize
  - NoticeRepository needs `findByLinkIn` method for batch lookup

  **Acceptance Criteria**:

  **If TDD**:
  - [ ] Test: `findByLinkIn` called once per chunk instead of `findByLink` per notice
  - [ ] Test: existing notices are updated, new notices are created
  - [ ] Test: `existsByLink` no longer called in scrape path
  - [ ] `./gradlew test` → ALL tests pass

  **QA Scenarios (MANDATORY)**:

  ```
  Scenario: Batch link check reduces DB queries
    Tool: Bash (test with repository mock)
    Preconditions: Chunk of 10 notices, 7 existing, 3 new
    Steps:
      1. Scrape a chunk with 10 notices
      2. Count repository calls: findByLinkIn, existsByLink, findByLink
      3. Assert findByLinkIn called once, existsByLink called zero times
    Expected Result: Single batch query replaces 10 individual queries
    Failure Indicators: existsByLink still called, or findByLink called individually
    Evidence: .sisyphus/evidence/task-9-batch-links.txt

  Scenario: New and existing notices handled correctly
    Tool: Bash (test)
    Preconditions: 3 existing notices in DB, chunk contains 5 (3 existing + 2 new)
    Steps:
      1. Run scrape/persist for the chunk
      2. Assert: 3 notices updated, 2 new notices created
      3. Assert: no duplicates created
    Expected Result: Correct create/update counts, no duplicate entries
    Failure Indicators: Duplicate notices in DB, missed updates, data loss
    Evidence: .sisyphus/evidence/task-9-correct-upsert.txt
  ```

  **Commit**: YES
  - Message: `perf(persist): batch notice link checks and use saveAll`
  - Files: `NoticeScrapeService.java`, `NoticePersistenceService.java`, `NoticeRepository.java`, test files
  - Pre-commit: `./gradlew test`

- [ ] 10. FCM Batch Sending

  **What to do**:
  - RED: Write test that verifies `FirebaseMessaging.sendEach` (or `sendMulticast`) is called instead of individual `send` calls when sending to multiple users.
  - GREEN: In `PushNotificationService`, collect `Message` objects for all target users, then call `FirebaseMessaging.getInstance().sendEach(messages)` (or batch of 500). Log `BatchResponse` failures for each message ID. In `KeywordService.onNoticeSaved()`, replace the per-user loop with batch message creation + single `sendEach` call.
  - REFACTOR: Keep `sendToTopic` for topic-based sends (this is already batched by FCM). Only batch the per-user topic sends.

  **Must NOT do**:
  - Do not build a retry queue or delivery tracking system
  - Do not change the `sendToTopic` method (it already uses FCM topic batching)
  - Do not add Firebase Admin SDK dependency changes (already at 9.4.2)

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with T9)
  - **Parallel Group**: Wave 3
  - **Blocks**: T11
  - **Blocked By**: None (independent of T7-T9)

  **References**:

  **Pattern References**:
  - `src/main/java/sulhoe/aura/service/firebase/PushNotificationService.java:53-84` — current `sendToTopic` with individual `FirebaseMessaging.getInstance().send(message)` for per-user sends
  - `src/main/java/sulhoe/aura/service/keyword/KeywordService.java:398-408` — per-user FCM send loop

  **API/Type References**:
  - Firebase Admin SDK 9.4.2: `FirebaseMessaging.getInstance().sendEach(messages)` or `sendMulticast(multicastMessage)`

  **External References**:
  - Firebase Admin Java SDK docs: `https://firebase.google.com/docs/reference/admin/java/reference/com/google/firebase/messaging/FirebaseMessaging#sendEach(java.util.List)` — batch send API

  **WHY Each Reference Matters**:
  - PushNotificationService is where individual `send()` calls happen — must change to `sendEach()`
  - KeywordService.onNoticeSaved has the per-user loop that calls `sendToUserTopic` — must be refactored to batch
  - Firebase Admin SDK 9.4.2 supports `sendEach()` natively

  **Acceptance Criteria**:

  **If TDD**:
  - [ ] Test: `sendEach` called once for N users instead of `send` called N times
  - [ ] Test: `BatchResponse` failures are logged, not thrown
  - [ ] `./gradlew test` → ALL tests pass

  **QA Scenarios (MANDATORY)**:

  ```
  Scenario: FCM sends batched, not per-user
    Tool: Bash (test with mock FirebaseMessaging)
    Preconditions: 5 users match a keyword
    Steps:
      1. Trigger fanout for a notice
      2. Verify FirebaseMessaging.sendEach() called once with 5 messages
      3. Verify FirebaseMessaging.send() NOT called individually
    Expected Result: Single batched call instead of 5 individual calls
    Failure Indicators: 5 individual send() calls (batching not applied)
    Evidence: .sisyphus/evidence/task-10-batch-fcm.txt

  Scenario: Partial FCM failure handled gracefully
    Tool: Bash (test)
    Preconditions: 5 users, 2 have invalid FCM tokens
    Steps:
      1. Mock FirebaseMessaging.sendEach() to return BatchResponse with 2 failures
      2. Trigger fanout
      3. Verify app logs the 2 failures and continues
      4. Verify no exception propagated
    Expected Result: Failures logged, successful messages sent, no crash
    Failure Indicators: Exception thrown on partial failure, or silent failure with no logging
    Evidence: .sisyphus/evidence/task-10-partial-failure.txt
  ```

  **Commit**: YES
  - Message: `perf(push): batch FCM sends with sendEach`
  - Files: `PushNotificationService.java`, `KeywordService.java`, test files
  - Pre-commit: `./gradlew test`

- [ ] 11. Security Integration Test Suite

  **What to do**:
  - Create comprehensive integration tests that verify the security changes work together end-to-end:
    - CSRF: cookie-auth POST requires token, Bearer POST exempt
    - RBAC: regular user → admin endpoint → 403; admin user → admin endpoint → success
    - Token hashing: refresh cycle works with SHA-256 hashed stored tokens
    - SSO bounds: 10,001st ticket rejected
    - OAuth state: invalid state rejected
  - Use `@SpringBootTest` with full security filter chain (not standalone MockMvc).
  - Tests should cover happy path AND error cases.

  **Must NOT do**:
  - Do not create a full E2E browser test suite
  - Do not test internal implementation details — test behavior only
  - Do not add Testcontainers or Docker dependencies

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: NO (depends on T1, T2, T5, T9, T10)
  - **Parallel Group**: Wave 4
  - **Blocks**: F1-F4
  - **Blocked By**: T1, T2, T5, T9, T10

  **References**:

  **Pattern References**:
  - `src/test/java/sulhoe/aura/controller/AuthControllerTest.java` — existing standalone MockMvc test, shows test style
  - `src/main/java/sulhoe/aura/config/SecurityConfig.java` — full security config to load

  **API/Type References**:
  - All modified security files: SecurityConfig, JwtAuthenticationFilter, SsoTicketService, AuthService, AuthController

  **WHY Each Reference Matters**:
  - AuthControllerTest shows the MockMvc pattern — but we need full context MockMvc
  - SecurityConfig must be loaded for CSRF/RBAC tests to be meaningful

  **Acceptance Criteria**:

  **If TDD**:
  - [ ] Test file: `src/test/java/sulhoe/aura/security/SecurityIntegrationTest.java`
  - [ ] Test: CSRF token required for cookie-auth POST to `/api/user/departments`
  - [ ] Test: Bearer token works without CSRF for `/api/auth/app/exchange`
  - [ ] Test: Regular user → 403 on `/api/admin/push/broadcast`
  - [ ] Test: Admin user → access on `/api/admin/push/broadcast`
  - [ ] Test: Invalid OAuth state rejected (401)
  - [ ] `./gradlew test` → ALL tests pass

  **QA Scenarios (MANDATORY)**:

  ```
  Scenario: Full security chain works end-to-end
    Tool: MockMvc (Spring Boot integration test)
    Preconditions: Full app context loaded with H2
    Steps:
      1. Create regular user JWT via test helper (JwtTokenProvider.generateToken)
      2. GET /api/user/info with regular JWT → 200
      3. POST /api/admin/push/topic with regular JWT → 403
      4. GET /api/user/info without JWT → 401
      5. POST /api/user/departments with cookie auth but no CSRF token → 403
      6. POST /api/user/departments with cookie auth + CSRF token → 200
    Expected Result: Each scenario produces the expected status code
    Failure Indicators: 200 on admin endpoint for regular user, 403 on Bearer endpoint
    Evidence: .sisyphus/evidence/task-11-security-chain.txt

  Scenario: SSO ticket bounds enforced in integration
    Tool: JUnit test (Spring Boot integration)
    Preconditions: Full app context, SsoTicketService with max 10000 tickets
    Steps:
      1. Create 10000 valid tickets via SsoTicketService
      2. Attempt to create ticket 10001
      3. Assert rejection (ApiException with appropriate status)
      4. Call cleanupExpired() after TTL expires
      5. Assert new tickets can be created after eviction
    Expected Result: Bounding works, eviction works
    Failure Indicators: No bound enforced, or eviction never runs
    Evidence: .sisyphus/evidence/task-11-sso-bounds.txt
  ```

  **Commit**: YES
  - Message: `test(security): add integration tests for CSRF, RBAC, token hashing, SSO bounds`
  - Files: `src/test/java/sulhoe/aura/security/SecurityIntegrationTest.java`, `src/test/resources/application-test.properties`
  - Pre-commit: `./gradlew test`

---

## Final Verification Wave (MANDATORY — after ALL implementation tasks)

> 4 review agents run in PARALLEL. ALL must APPROVE. Present consolidated results to user and get explicit "okay" before completing.

- [ ] F1. **Plan Compliance Audit** — `oracle`
  Read the plan end-to-end. For each "Must Have": verify implementation exists (read file, curl endpoint, run command). For each "Must NOT Have": search codebase for forbidden patterns — reject with file:line if found. Check evidence files exist in `.sisyphus/evidence/`. Compare deliverables against plan.
  Output: `Must Have [N/N] | Must NOT Have [N/N] | Tasks [N/N] | VERDICT: APPROVE/REJECT`

- [ ] F2. **Code Quality Review** — `unspecified-high`
  Run `./gradlew test` and `./gradlew build`. Review all changed files for: `as any`/`@ts-ignore` equivalents in Java (raw types, unchecked casts, `@SuppressWarnings`), empty catches, `System.out.println` in prod, commented-out code, unused imports. Check AI slop: excessive comments, over-abstraction, generic names. Verify no new security anti-patterns (hardcoded secrets, plaintext tokens, missing CSRF).
  Output: `Build [PASS/FAIL] | Tests [N pass/N fail] | Files [N clean/N issues] | VERDICT`

- [ ] F3. **Real Manual QA** — `unspecified-high`
  Start from clean state (`./gradlew bootRun`). Execute EVERY QA scenario from EVERY task — follow exact steps, capture evidence. Test cross-task integration: CSRF token flow end-to-end, admin 403 for regular user, lazy loading with API calls, batch FCM with mock. Save to `.sisyphus/evidence/final-qa/`.
  Output: `Scenarios [N/N pass] | Integration [N/N] | Edge Cases [N tested] | VERDICT`

- [ ] F4. **Scope Fidelity Check** — `deep`
  For each task: read "What to do", read actual diff (`git log/diff`). Verify 1:1 — everything in spec was built (no missing), nothing beyond spec was built (no creep). Check "Must NOT do" compliance. Detect cross-task contamination: Task N touching Task M's files. Flag unaccounted changes.
  Output: `Tasks [N/N compliant] | Contamination [CLEAN/N issues] | Unaccounted [CLEAN/N files] | VERDICT`

---

## Commit Strategy

- **T0**: `test(infra): add H2 test dependency and security test base config`
- **T1**: `feat(security): add ROLE_ADMIN to JWT and protect admin endpoints`
- **T2**: `feat(security): hash refresh tokens with SHA-256 before DB storage`
- **T3**: `feat(security): add cryptographic nonce to OAuth state parameter`
- **T4**: `fix(security): bound SSO ticket store and add scheduled eviction`
- **T5**: `fix(security): restrict CSRF exemption to Bearer-only endpoints`
- **T6**: `refactor(scrape): make backfill configurable via app.notice.force-backfill`
- **T7**: `perf(entity): change User.departments to LAZY fetch with @Transactional`
- **T8**: `perf(keyword): reuse global keyword cache in fanout path`
- **T9**: `perf(persist): batch notice link checks and use saveAll`
- **T10**: `perf(push): batch FCM sends with sendEach`
- **T11**: `test(security): add integration tests for CSRF, RBAC, token hashing, SSO bounds`

---

## Success Criteria

### Verification Commands
```bash
./gradlew test                                    # Expected: all tests pass
./gradlew bootRun                                 # Expected: app starts without error
curl -s http://localhost:8080/api/auth/csrf        # Expected: CSRF token cookie returned
curl -s -X POST http://localhost:8080/api/user/departments -H "Cookie: WEB_SESSION=..."  # Expected: 403 without CSRF token
```

### Final Checklist
- [ ] All "Must Have" present
- [ ] All "Must NOT Have" absent
- [ ] All tests pass