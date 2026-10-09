package io.github.pasindu9999.orderflow.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.pasindu9999.orderflow.contracts.Message;
import io.github.pasindu9999.orderflow.contracts.MessageCatalog;
import io.github.pasindu9999.orderflow.contracts.inventory.InventoryRejected;
import io.github.pasindu9999.orderflow.contracts.inventory.InventoryReserved;
import io.github.pasindu9999.orderflow.contracts.inventory.ReleaseInventory;
import io.github.pasindu9999.orderflow.contracts.inventory.ReserveInventory;
import io.github.pasindu9999.orderflow.contracts.payment.PaymentFailed;
import io.github.pasindu9999.orderflow.contracts.payment.PaymentRefunded;
import io.github.pasindu9999.orderflow.contracts.payment.PaymentSucceeded;
import io.github.pasindu9999.orderflow.contracts.payment.ProcessPayment;
import io.github.pasindu9999.orderflow.contracts.payment.RefundPayment;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Golden-file contract tests (ADR-0005). Each file under {@code src/test/resources/contracts/} is the committed
 * wire format of one message type and version.
 *
 * <p><b>Never edit a golden file to make a test pass.</b> If one of these tests fails, the change you made is a
 * breaking wire change. Either revert it, or add a new schema version with a new golden file.
 */
class ContractGoldenFileTest {

    static final UUID ORDER_ID = UUID.fromString("3f2b6c1e-0d4a-4a8e-9b6f-1c2d3e4f5a6b");
    static final UUID MESSAGE_ID = UUID.fromString("7d3f1c2e-9a51-4b0e-8f0a-2a6a1f6f9c11");
    static final UUID CAUSATION_ID = UUID.fromString("5a0e7b9c-2d1f-4c3b-8a6e-9f8d7c6b5a40");
    static final UUID CUSTOMER_ID = UUID.fromString("c0ffee00-1234-4abc-8def-000000000001");
    static final UUID PAYMENT_ID = UUID.fromString("9b8a7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d");
    static final Instant OCCURRED_AT = Instant.parse("2026-10-08T10:15:30.123Z");
    static final Instant EXPIRES_AT = Instant.parse("2026-10-08T10:16:00Z");

    /** One sample payload per message type, matching the values in its golden file. */
    static final Map<Class<? extends Message>, Message> SAMPLES = Stream.of(
                    new ReserveInventory(ORDER_ID, List.of(
                            new ReserveInventory.Line("MUG-RED", 2),
                            new ReserveInventory.Line("TEA-GREEN", 1))),
                    new ReleaseInventory(ORDER_ID, ReleaseInventory.Reason.PAYMENT_DECLINED),
                    new InventoryReserved(ORDER_ID),
                    new InventoryRejected(ORDER_ID, InventoryRejected.Reason.OUT_OF_STOCK, List.of(
                            new InventoryRejected.Shortage("MUG-RED", 2, 1))),
                    new ProcessPayment(ORDER_ID, CUSTOMER_ID, new BigDecimal("37.50"), "EUR", EXPIRES_AT),
                    new RefundPayment(ORDER_ID, RefundPayment.Reason.LATE_PAYMENT),
                    new PaymentSucceeded(ORDER_ID, PAYMENT_ID, new BigDecimal("37.50"), "EUR"),
                    new PaymentFailed(ORDER_ID, PaymentFailed.Reason.DECLINED_LIMIT),
                    new PaymentRefunded(ORDER_ID, PAYMENT_ID))
            .collect(Collectors.toMap(Message::getClass, m -> m));

    final MessageCodec codec = new MessageCodec(MessageCatalog.ENTRIES, "contract-test", Clock.systemUTC());
    final JsonMapper plainJson = new JsonMapper();

    static Stream<MessageCatalog.Entry> catalog() {
        return MessageCatalog.ENTRIES.stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("catalog")
    void shouldSerializeExactlyAsGoldenFile_whenEncodingSample(MessageCatalog.Entry entry) {
        JsonNode encoded = plainJson.readTree(codec.encode(sampleEnvelope(entry)));

        assertThat(encoded).isEqualTo(golden(entry));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("catalog")
    void shouldDecodeGoldenFile_whenReadingCommittedContract(MessageCatalog.Entry entry) {
        Envelope decoded = codec.decode(golden(entry).toString());

        assertThat(decoded).isEqualTo(sampleEnvelope(entry));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("catalog")
    void shouldIgnoreUnknownFields_whenGoldenFileHasExtraFields(MessageCatalog.Entry entry) {
        ObjectNode withExtras = (ObjectNode) golden(entry);
        withExtras.put("addedByNewerProducer", "ignored");
        ((ObjectNode) withExtras.get("payload")).put("optionalFieldFromTheFuture", 42);

        Envelope decoded = codec.decode(withExtras.toString());

        assertThat(decoded).isEqualTo(sampleEnvelope(entry));
    }

    @Test
    void shouldHaveSampleForEveryCatalogEntry_whenNewMessageTypeIsAdded() {
        assertThat(SAMPLES.keySet())
                .containsExactlyInAnyOrderElementsOf(MessageCatalog.ENTRIES.stream().map(MessageCatalog.Entry::payloadClass).toList());
    }

    static Envelope sampleEnvelope(MessageCatalog.Entry entry) {
        // The first command of a saga has no cause; everything after it does.
        UUID causationId = entry.payloadClass() == ReserveInventory.class ? null : CAUSATION_ID;
        return new Envelope(MESSAGE_ID, entry.type(), entry.version(), OCCURRED_AT, ORDER_ID, causationId,
                "contract-test", SAMPLES.get(entry.payloadClass()));
    }

    JsonNode golden(MessageCatalog.Entry entry) {
        String path = "/contracts/%s.v%d.json".formatted(entry.type(), entry.version());
        try (InputStream in = getClass().getResourceAsStream(path)) {
            assertThat(in).as("golden file %s", path).isNotNull();
            return plainJson.readTree(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
