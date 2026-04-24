# AGENTS.md

## Project shape
- Single-module Gradle Spring Boot backend. Use the Gradle wrapper from the repo root; do not assume a monorepo, package workspace, or CI helper scripts.
- Toolchain is pinned in code: Java 21, Spring Boot 3.4.5, Gradle wrapper 8.13 (`build.gradle`, `gradle/wrapper/gradle-wrapper.properties`).

## Canonical commands
- Run tests with `./gradlew test`.
- Build with `./gradlew build`.
- Run locally with `./gradlew bootRun`.
- Prefer `./gradlew <task>` over a globally installed `gradle`.

## Routing and entrypoints
- All `@RestController` routes are prefixed with `/api` centrally in `src/main/java/sulhoe/aura/config/WebConfig.java`. Declared controller mappings like `/auth` become runtime paths like `/api/auth`.
- App entrypoint is `src/main/java/sulhoe/aura/AuraApplication.java`.
- Startup has side effects: `KeywordService.initOnReady()` seeds global keywords and can retag all notices when `app.keywords.retag-on-start=true`.
- Background scraping starts automatically: `ScrapeScheduleService.runNoticeScraping()` runs 5 seconds after boot and then every 5 minutes.

## High-signal architecture
- HTTP surface lives under `src/main/java/sulhoe/aura/controller/`.
- Core business logic is split mainly across `service/login`, `service/notice`, `service/keyword`, and `service/firebase`.
- Notice scraping is config-driven, not hardcoded: URLs, labels, parser bean names, and posted-date exceptions come from `src/main/resources/application.properties` and `src/main/resources/application.yml` through `NoticeConfig`.
- The important notice pipeline is: scheduled scrape -> parser-selected fetch/parse -> persistence -> fanout -> keyword/subscription matching -> Firebase push delivery.

## Auth and client-flow quirks
- Auth supports both browser and native app flows. `AuthController` branches on `state=app`; native clients use the one-time SSO ticket exchange at `/api/auth/app/exchange`.
- The mobile auth flow is documented in `docs/react-native-auth.md`; check it before changing auth endpoints or redirect behavior.
- Browser auth relies on secure cookies (`refreshToken`, `WEB_SESSION`) with `SameSite=None`. JWT resolution prefers the `WEB_SESSION` cookie before `Authorization: Bearer`.

## Config and environment notes
- Runtime secrets and service endpoints are env-driven through `src/main/resources/application.properties`; do not hardcode values that are already configured there.
- Firebase bootstrap expects `firebase.service.account.json`, with optional base64 input controlled by `firebase.service.account.json.base64`.
- CORS origins come from a comma-separated `cors.allowed-origins`; `FRONTEND_URL` is only the fallback.
- A root `.env` file exists in the repo. Treat it as local secret material and avoid including its contents in commits or summaries.

## Testing reality
- Current test coverage is minimal. The visible test file is `src/test/java/sulhoe/aura/controller/AuthControllerTest.java`, and it uses standalone `MockMvc` with mocked collaborators rather than a full Spring context.
- For auth changes, start by extending or running that focused test before attempting broader validation.

## Repo-specific gotchas
- `README.md` is UTF-16LE, so plain text tooling may display it incorrectly unless converted first.
- There is no repo-local CI workflow, Makefile, task runner, or alternate instruction file to rely on; the Gradle wrapper and source config files are the source of truth.
