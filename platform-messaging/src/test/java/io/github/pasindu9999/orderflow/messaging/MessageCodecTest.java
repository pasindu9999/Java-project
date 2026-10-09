package io.github.pasindu9999.orderflow.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.pasindu9999.orderflow.contracts.Message;
import io.github.pasindu9999.orderflow.contracts.MessageCatalog;
import io.github.pasindu9999.orderflow.contracts.inventory.InventoryCommand;
import io.github.pasindu9999.orderflow.contracts.inventory.ReserveInventory;
import io.github.pasindu9999.orderflow.contracts.payment.PaymentCommand;
import io.github.pasindu9999.orderflow.contracts.payment.PaymentFailed;
import io.github.pasindu9999.orderflow.contracts.payment.ProcessPayment;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

class MessageCodecTest {

    static final Instant NOW = Instant.parse("2026-10-09T08:00:00Z");

    final MessageCodec codec = new MessageCodec(MessageCatalog.ENTRIES, "order-service", Clock.fixed(NOW, ZoneOffset.UTC));
    final JsonMapper plainJson = new JsonMapper();
    final UUID orderId = UUID.randomUUID();

    @Test
    void shouldFillEnvelopeFromCatalogAndClock_whenWrappingPayload() {
        UUID causationId = UUID.randomUUID();

        Envelope envelope = codec.wrap(new ReserveInventory(orderId, List.of(new ReserveInventory.Line("MUG-RED", 1))), causationId);

        assertThat(envelope.messageId()).isNotNull();
        assertThat(envelope.messageType()).isEqualTo("ReserveInventory");
        assertThat(envelope.schemaVersion()).isEqualTo(1);
        assertThat(envelope.occurredAt()).isEqualTo(NOW);
        assertThat(envelope.correlationId()).isEqualTo(orderId);
        assertThat(envelope.causationId()).isEqualTo(causationId);
        assertThat(envelope.producer()).isEqualTo("order-service");
    }

    @Test
    void shouldReturnEqualEnvelope_whenEncodedThenDecoded() {
        Envelope original = codec.wrap(processPayment("37.50"), null);

        assertThat(codec.decode(codec.encode(original))).isEqualTo(original);
    }

    @Test
    void shouldGiveEachWrappedMessageItsOwnId_whenWrappingTwice() {
        Message payload = processPayment("1.00");

        assertThat(codec.wrap(payload, null).messageId()).isNotEqualTo(codec.wrap(payload, null).messageId());
    }

    @Test
    void shouldWriteMoneyAsPlainDecimalString_whenAmountHasExponentForm() {
        JsonNode payload = encodedPayload(processPayment("1E+3"));

        assertThat(payload.get("amount").isString()).isTrue();
        assertThat(payload.get("amount").asString()).isEqualTo("1000");
    }

    @Test
    void shouldKeepScaleOfMoney_whenRoundTripping() {
        Envelope decoded = codec.decode(codec.encode(codec.wrap(processPayment("37.50"), null)));

        assertThat(((ProcessPayment) decoded.payload()).amount()).isEqualByComparingTo("37.50").hasScaleOf(2);
    }

    @Test
    void shouldRejectRegistrationGap_whenWrappingPayloadOutsideCatalog() {
        record NotInCatalog(UUID orderId) implements Message {
        }

        assertThatThrownBy(() -> codec.wrap(new NotInCatalog(orderId), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   ", "not json", "{\"messageId\":", "[1,2,3]", "null"})
    void shouldThrowParseException_whenInputIsNotAnEnvelope(String input) {
        assertThatThrownBy(() -> codec.decode(input)).isInstanceOf(MessageParseException.class);
    }

    @Test
    void shouldThrowParseException_whenInputIsNull() {
        assertThatThrownBy(() -> codec.decode(null)).isInstanceOf(MessageParseException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"messageId", "messageType", "schemaVersion", "occurredAt", "correlationId", "payload"})
    void shouldThrowParseException_whenRequiredEnvelopeFieldIsMissing(String field) {
        ObjectNode json = validEnvelopeJson();
        json.remove(field);

        assertThatThrownBy(() -> codec.decode(json.toString()))
                .isInstanceOf(MessageParseException.class)
                .hasMessageContaining(field);
    }

    @Test
    void shouldThrowParseException_whenMessageTypeIsUnknown() {
        ObjectNode json = validEnvelopeJson().put("messageType", "ShipOrder");

        assertThatThrownBy(() -> codec.decode(json.toString()))
                .isInstanceOf(MessageParseException.class)
                .hasMessageContaining("Unsupported message type ShipOrder v1");
    }

    @Test
    void shouldThrowParseException_whenSchemaVersionIsNotSupported() {
        ObjectNode json = validEnvelopeJson().put("schemaVersion", 2);

        assertThatThrownBy(() -> codec.decode(json.toString()))
                .isInstanceOf(MessageParseException.class)
                .hasMessageContaining("Unsupported message type ProcessPayment v2");
    }

    @Test
    void shouldThrowParseException_whenPayloadHasWrongShape() {
        ObjectNode json = validEnvelopeJson();
        ((ObjectNode) json.get("payload")).put("expiresAt", "next tuesday");

        assertThatThrownBy(() -> codec.decode(json.toString()))
                .isInstanceOf(MessageParseException.class)
                .hasMessageContaining("Invalid ProcessPayment v1 payload");
    }

    @Test
    void shouldThrowParseException_whenEnumValueIsUnknown() {
        ObjectNode json = plainJson.readValue(
                codec.encode(codec.wrap(new PaymentFailed(orderId, PaymentFailed.Reason.EXPIRED), null)), ObjectNode.class);
        ((ObjectNode) json.get("payload")).put("reason", "DECLINED_BY_MOON_PHASE");

        assertThatThrownBy(() -> codec.decode(json.toString())).isInstanceOf(MessageParseException.class);
    }

    @Test
    void shouldThrowParseException_whenPayloadHasNoOrderId() {
        ObjectNode json = validEnvelopeJson();
        ((ObjectNode) json.get("payload")).remove("orderId");

        assertThatThrownBy(() -> codec.decode(json.toString()))
                .isInstanceOf(MessageParseException.class)
                .hasMessageContaining("payload.orderId");
    }

    @Test
    void shouldAcceptMoneyAsJsonNumber_whenOlderOrForeignProducerSendsIt() {
        ObjectNode json = validEnvelopeJson();
        ((ObjectNode) json.get("payload")).put("amount", new BigDecimal("12.5"));

        ProcessPayment payload = (ProcessPayment) codec.decode(json.toString()).payload();

        assertThat(payload.amount()).isEqualByComparingTo("12.50");
    }

    @Test
    void shouldNarrowPayload_whenTypeMatchesTopic() {
        Envelope envelope = codec.decode(codec.encode(codec.wrap(processPayment("5.00"), null)));

        assertThat(envelope.payloadAs(PaymentCommand.class)).isInstanceOf(ProcessPayment.class);
    }

    @Test
    void shouldThrowParseException_whenPayloadDoesNotBelongOnTopic() {
        Envelope envelope = codec.wrap(processPayment("5.00"), null);

        assertThatThrownBy(() -> envelope.payloadAs(InventoryCommand.class))
                .isInstanceOf(MessageParseException.class)
                .hasMessageContaining("Expected InventoryCommand but got ProcessPayment v1");
    }

    private ProcessPayment processPayment(String amount) {
        return new ProcessPayment(orderId, UUID.randomUUID(), new BigDecimal(amount), "EUR", NOW.plusSeconds(30));
    }

    private JsonNode encodedPayload(Message payload) {
        return plainJson.readTree(codec.encode(codec.wrap(payload, null))).get("payload");
    }

    private ObjectNode validEnvelopeJson() {
        return plainJson.readValue(codec.encode(codec.wrap(processPayment("12.50"), null)), ObjectNode.class);
    }
}
