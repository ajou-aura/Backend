// sulhoe.aura.service.login.SsoTicketService
package sulhoe.aura.service.login;

import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class SsoTicketService {
    private static final long TTL_MS = 120_000; // 2분
    private final ConcurrentHashMap<String, Entry> store = new ConcurrentHashMap<>();

    /** email/name/signUp을 직접 받아 1회용 티켓을 발급 */
    public String issue(String email, String name, boolean signUp) {
        String code = java.util.UUID.randomUUID().toString();
        store.put(code, new Entry(new Payload(email, name, signUp),
                System.currentTimeMillis() + TTL_MS));
        return code;
    }

    /** 사용 시 삭제(consume) */
    public Payload consume(String code) {
        var e = store.remove(code);
        if (e == null || e.exp < System.currentTimeMillis()) return null;
        return e.payload;
    }

    private record Entry(Payload payload, long exp) {}
    public record Payload(String email, String name, boolean signUp) {}
}
