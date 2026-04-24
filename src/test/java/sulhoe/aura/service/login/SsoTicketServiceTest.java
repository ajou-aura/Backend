package sulhoe.aura.service.login;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SsoTicketServiceTest {

    private SsoTicketService service;

    @BeforeEach
    void setUp() {
        service = new SsoTicketService();
    }

    @Test
    void issueAndConsumeWorks() {
        String ticket = service.issue("user@test.com", "Test User", false);
        var payload = service.consume(ticket);

        assertThat(payload.email()).isEqualTo("user@test.com");
        assertThat(payload.name()).isEqualTo("Test User");
        assertThat(payload.signUp()).isFalse();
    }

    @Test
    void consumeReturnsNullForNonexistent() {
        assertThat(service.consume("nonexistent")).isNull();
    }

    @Test
    void consumeInvalidatesUsedTicket() {
        String ticket = service.issue("user@test.com", "Test User", false);
        service.consume(ticket);
        assertThat(service.consume(ticket)).isNull();
    }

    @Test
    void ticket10001IsRejectedAtCapacity() {
        for (int i = 0; i < 10_000; i++) {
            service.issue("user" + i + "@test.com", "User" + i, false);
        }

        assertThatThrownBy(() -> service.issue("overflow@test.com", "Overflow User", false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Ticket store at capacity");
    }

    @Test
    void generateAndConsumeStateNonceWorks() {
        String nonce = service.generateStateNonce("web");
        assertThat(nonce).isNotNull();
        assertThat(nonce).isNotEmpty();

        String mode = service.consumeStateNonce(nonce);
        assertThat(mode).isEqualTo("web");
    }

    @Test
    void consumeStateNonceReturnsNullForNonexistent() {
        assertThat(service.consumeStateNonce("nonexistent-nonce")).isNull();
    }

    @Test
    void consumeStateNonceReturnsNullForUsedNonce() {
        String nonce = service.generateStateNonce("app");
        assertThat(service.consumeStateNonce(nonce)).isEqualTo("app");
        assertThat(service.consumeStateNonce(nonce)).isNull();
    }
}