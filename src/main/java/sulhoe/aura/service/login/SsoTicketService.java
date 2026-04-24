package sulhoe.aura.service.login;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SsoTicketService {
    private static final int MAX_SIZE = 10_000;
    private static final long TTL_MS = 120_000;
    private static final long NONCE_TTL_MS = 120_000;

    private final ConcurrentHashMap<String, Entry> store = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, StateEntry> stateStore = new ConcurrentHashMap<>();
    private final SecureRandom secureRandom = new SecureRandom();

    public String issue(String email, String name, boolean signUp) {
        cleanupExpired();
        if (store.size() >= MAX_SIZE) {
            throw new IllegalStateException("Ticket store at capacity");
        }

        String code = UUID.randomUUID().toString();
        store.put(code, new Entry(new Payload(email, name, signUp), System.currentTimeMillis() + TTL_MS));
        return code;
    }

    public Payload consume(String code) {
        Entry entry = store.remove(code);
        if (entry == null || entry.exp < System.currentTimeMillis()) {
            return null;
        }
        return entry.payload;
    }

    public String consumeStateNonce(String nonce) {
        StateEntry entry = stateStore.remove(nonce);
        if (entry == null || entry.exp < System.currentTimeMillis()) {
            return null;
        }
        return entry.mode;
    }

    public String generateStateNonce(String mode) {
        byte[] nonceBytes = new byte[32];
        secureRandom.nextBytes(nonceBytes);
        String nonce = Base64.getUrlEncoder().withoutPadding().encodeToString(nonceBytes);
        stateStore.put(nonce, new StateEntry(mode, System.currentTimeMillis() + NONCE_TTL_MS));
        return nonce;
    }

    @Scheduled(fixedDelay = 60_000)
    public void cleanupExpired() {
        long now = System.currentTimeMillis();
        store.entrySet().removeIf(entry -> entry.getValue().exp < now);
        stateStore.entrySet().removeIf(entry -> entry.getValue().exp < now);
    }

    record Entry(Payload payload, long exp) {}

    private record StateEntry(String mode, long exp) {}

    public record Payload(String email, String name, boolean signUp) {}
}
