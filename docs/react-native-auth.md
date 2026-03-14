# React Native Auth Flow

`/api/auth/sso/bridge` is a legacy WebView bridge. Pure React Native clients should use the app exchange flow below and keep tokens in native secure storage.

## Sequence

1. Open `GET /api/auth/google?mode=app`.
2. Google redirects to `GET /api/auth/callback?state=app&code=...`.
3. The backend redirects once to `aura://oauth-callback?code=<sso_ticket>`.
4. The app posts that ticket to `POST /api/auth/app/exchange`.
5. The backend returns JSON `accessToken`, `refreshToken`, `signUp`, and `user`.
6. When the access token expires, the app calls `POST /api/auth/refresh` with `{ "refreshToken": "..." }`.
7. On sign-out, the app calls `POST /api/auth/logout` with `{ "refreshToken": "..." }`.

## App Exchange

Request:

```http
POST /api/auth/app/exchange
Content-Type: application/json

{
  "code": "<sso_ticket>"
}
```

Response:

```json
{
  "status": "success",
  "code": 200,
  "message": "요청이 성공적으로 처리되었습니다.",
  "data": {
    "accessToken": "...",
    "refreshToken": "...",
    "signUp": true,
    "user": {
      "email": "user@ajou.ac.kr",
      "name": "Aura User"
    }
  }
}
```

Invalid, reused, or expired tickets return `401` with `INVALID_SSO_TICKET`.

## Refresh And Logout

`POST /api/auth/refresh` accepts the refresh token from either the `refreshToken` cookie or a JSON body. Native apps should send the body form:

```json
{
  "refreshToken": "..."
}
```

`POST /api/auth/logout` also accepts the same body and revokes the stored refresh token in the database before clearing auth cookies.

## Legacy Bridge

`GET /api/auth/sso/bridge?code=...` remains available for web preview and old WebView-based flows. Native clients do not need it.

## CORS / Expo Dev Origins

The backend still reads `cors.allowed-origins`, but it can now be supplied as a comma-separated environment variable:

```env
FRONTEND_URL=https://aura.example.com
CORS_ALLOWED_ORIGINS=https://aura.example.com,http://localhost:3000,http://localhost:19006,http://127.0.0.1:19006
```

`FRONTEND_URL` stays as the fallback origin. Add Expo web or local preview origins through `CORS_ALLOWED_ORIGINS` instead of hardcoding them.
