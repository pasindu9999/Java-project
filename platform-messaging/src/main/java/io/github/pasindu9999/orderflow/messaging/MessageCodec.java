package io.github.pasindu9999.orderflow.messaging;

import io.github.pasindu9999.orderflow.contracts.Message;
import io.github.pasindu9999.orderflow.contracts.MessageCatalog;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.module.SimpleModule;
import tools.jackson.databind.ser.std.StdSerializer;

/**
 * Converts between {@link Envelope} and the JSON string sent over Kafka (ADR-0005).
 *
 * <p>The codec owns its own {@link JsonMapper} instead of using Spring's: the wire format must not change
 * because someone tunes {@code spring.jackson.*} for the REST API. Every decoding problem surfaces as a
 * {@link MessageParseException}, which the error handler sends straight to the DLT.
 */
public final class MessageCodec {

    private record TypeKey(String type, int version) {
    }

    /** What actually goes over the wire: the payload stays raw JSON until type and version are known. */
    private record WireEnvelope(
            UUID messageId,
            String messageType,
            Integer schemaVersion,
            Instant occurredAt,
            UUID correlationId,
            UUID causationId,
            String producer,
            JsonNode payload) {
    }

    private final JsonMapper mapper = wireMapper();
    private final Map<TypeKey, Class<? extends Message>> classByType;
    private final Map<Class<? extends Message>, MessageCatalog.Entry> entryByClass;
    private final String producer;
    private final Clock clock;

    public MessageCodec(List<MessageCatalog.Entry> catalog, String producer, Clock clock) {
        this.classByType = catalog.stream()
                .collect(Collectors.toUnmodifiableMap(e -> new TypeKey(e.type(), e.version()), MessageCatalog.Entry::payloadClass));
        this.entryByClass = catalog.stream()
                .collect(Collectors.toUnmodifiableMap(MessageCatalog.Entry::payloadClass, Function.identity()));
        this.producer = producer;
        this.clock = clock;
    }

    /** Wraps a new outgoing message: fresh messageId, current time, type and version from the catalog. */
    public Envelope wrap(Message payload, UUID causationId) {
        MessageCatalog.Entry entry = entryFor(payload);
        return new Envelope(UUID.randomUUID(), entry.type(), entry.version(), clock.instant(),
                payload.orderId(), causationId, producer, payload);
    }

    /** The one topic this message type is published on. */
    public String topicFor(Message payload) {
        return entryFor(payload).topic();
    }

    private MessageCatalog.Entry entryFor(Message payload) {
        MessageCatalog.Entry entry = entryByClass.get(payload.getClass());
        if (entry == null) {
            throw new IllegalArgumentException("Not in the message catalog: " + payload.getClass().getName());
        }
        return entry;
    }

    public String encode(Envelope envelope) {
        var wire = new WireEnvelope(envelope.messageId(), envelope.messageType(), envelope.schemaVersion(),
                envelope.occurredAt(), envelope.correlationId(), envelope.causationId(), envelope.producer(),
                mapper.valueToTree(envelope.payload()));
        return mapper.writeValueAsString(wire);
    }

    public Envelope decode(String json) {
        if (json == null || json.isBlank()) {
            throw new MessageParseException("Empty message");
        }
        WireEnvelope wire;
        try {
            wire = mapper.readValue(json, WireEnvelope.class);
        } catch (JacksonException e) {
            throw new MessageParseException("Malformed envelope: " + e.getOriginalMessage(), e);
        }
        if (wire == null) {
            throw new MessageParseException("Message is JSON null");
        }
        require(wire.messageId(), "messageId");
        require(wire.messageType(), "messageType");
        require(wire.schemaVersion(), "schemaVersion");
        require(wire.occurredAt(), "occurredAt");
        require(wire.correlationId(), "correlationId");
        if (wire.payload() == null || wire.payload().isNull()) {
            throw new MessageParseException("Envelope field 'payload' is missing");
        }

        Class<? extends Message> payloadClass = classByType.get(new TypeKey(wire.messageType(), wire.schemaVersion()));
        if (payloadClass == null) {
            throw new MessageParseException(
                    "Unsupported message type %s v%d".formatted(wire.messageType(), wire.schemaVersion()));
        }
        Message payload;
        try {
            payload = mapper.treeToValue(wire.payload(), payloadClass);
        } catch (JacksonException e) {
            throw new MessageParseException(
                    "Invalid %s v%d payload: %s".formatted(wire.messageType(), wire.schemaVersion(), e.getOriginalMessage()), e);
        }
        require(payload.orderId(), "payload.orderId");

        return new Envelope(wire.messageId(), wire.messageType(), wire.schemaVersion(), wire.occurredAt(),
                wire.correlationId(), wire.causationId(), wire.producer(), payload);
    }

    private static void require(Object value, String field) {
        if (value == null) {
            throw new MessageParseException("Envelope field '" + field + "' is missing");
        }
    }

    private static JsonMapper wireMapper() {
        return JsonMapper.builder()
                // Tolerant reader: producers may add optional fields without breaking consumers (ADR-0005, rule 1).
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .disable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
                // Keep record component order so outbox rows and DLT records read naturally.
                .disable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
                .addModule(new SimpleModule("orderflow-wire").addSerializer(BigDecimal.class, new MoneyAsStringSerializer()))
                .build();
    }

    /** Money travels as a decimal string ("37.50"), never a JSON number, and never in exponent form ("1E+3"). */
    private static final class MoneyAsStringSerializer extends StdSerializer<BigDecimal> {

        MoneyAsStringSerializer() {
            super(BigDecimal.class);
        }

        @Override
        public void serialize(BigDecimal value, JsonGenerator gen, SerializationContext context) {
            gen.writeString(value.toPlainString());
        }
    }
}
