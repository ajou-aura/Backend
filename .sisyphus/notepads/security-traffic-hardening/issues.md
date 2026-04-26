- Verification was initially blocked by pre-existing corruption in `SsoTicketService` and `SsoTicketServiceTest`; both files had duplicated/broken content and were normalized so `./gradlew test` could compile and run.
- Gradle test reporting hit `TestOutputStore` XML generation EOF errors on repeated runs; adding `src/test/resources/logback-test.xml` with root logging disabled stabilized `./gradlew clean test build`.

## Code Quality Review Findings (2026-04-24)

### Minor Issues (non-blocking)
1. **AuthController.java:233** — Tab character `\t` in Korean error message string. Likely a typo; should be plain text.
2. **AuthController.java:285** — Empty catch block `catch (Exception ignored) {}` in `resolveModeFromState()`. Silently swallows all exceptions. Should at least log at DEBUG.
3. **AuthController.java:326** — Missing space before `{` in `try{` — minor formatting inconsistency.
4. **KeywordService.java:47** — `cachedGlobalNorms` field is not `volatile`. Potential visibility issue when accessed from scheduled fanout threads vs. `@EventListener` thread.
5. **KeywordService.java:378** — `getTypeMode()` returns `null` for unset preferences instead of Optional or default enum value.
6. **PushNotificationService.java:89** — String concatenation in `logger.error()` call. Should use parameterized logging to avoid unnecessary string construction.
7. **SsoTicketService.java:27** — Uses `UUID.randomUUID()` for ticket generation while having `SecureRandom` available for nonces. Not a security risk for one-time-use tickets but inconsistent.

### Security Review (PASS)
- No hardcoded production secrets (all use `${ENV_VAR}` substitution)
- Test properties contain fake values clearly marked for test-only use
- CSRF properly configured with `CookieCsrfTokenRepository`, `SameSite=None`, `Secure=true`
- Bearer requests correctly excluded from CSRF
- Refresh tokens stored as SHA-256 hashes (not plaintext)
- SSO nonces use `SecureRandom` with 32 bytes
- SSO tickets are one-time-use with capacity bound (10,000) and TTL (120s)
