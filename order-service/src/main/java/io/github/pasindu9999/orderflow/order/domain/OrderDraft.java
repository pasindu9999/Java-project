package io.github.pasindu9999.orderflow.order.domain;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Currency;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A validated, normalised order request, before it becomes an {@link Order}.
 *
 * <p>All order validation lives here rather than in Bean Validation annotations, so the rules sit in one place,
 * in plain Java, and need no extra dependency.
 */
public record OrderDraft(UUID customerId, String currency, List<OrderLine> lines) {

    public static final int MAX_LINES = 50;
    public static final int MAX_QUANTITY = 1_000;
    public static final int MAX_SKU_LENGTH = 64;

    /** Raw line input. Fields are nullable because they come straight from the client. */
    public record LineInput(String sku, Integer quantity, BigDecimal unitPrice) {
    }

    /** Validates the raw input and reports every violation at once, so clients can fix them in one round trip. */
    public static OrderDraft of(UUID customerId, String currency, List<LineInput> lines) {
        List<String> errors = new ArrayList<>();
        if (customerId == null) {
            errors.add("customerId is required");
        }
        validateCurrency(currency, errors);

        List<OrderLine> validLines = new ArrayList<>();
        if (lines == null || lines.isEmpty()) {
            errors.add("lines must contain at least one line");
        } else if (lines.size() > MAX_LINES) {
            errors.add("lines must contain at most " + MAX_LINES + " lines");
        } else {
            for (int i = 0; i < lines.size(); i++) {
                validateLine(i, lines.get(i), errors).ifPresent(validLines::add);
            }
        }

        if (errors.isEmpty() && total(validLines).signum() == 0) {
            errors.add("order total must be greater than 0");
        }
        if (!errors.isEmpty()) {
            throw new InvalidOrderException(errors);
        }
        return new OrderDraft(customerId, currency, List.copyOf(validLines));
    }

    public BigDecimal total() {
        return total(lines);
    }

    /**
     * SHA-256 of the normalised request. Two requests that mean the same thing ("12.5" vs "12.50") get the same
     * fingerprint, so a client retry is recognised as a replay rather than a conflicting reuse of the key.
     */
    public String fingerprint() {
        String canonical = customerId + "|" + currency + "|" + lines.stream()
                .map(l -> l.sku() + ":" + l.quantity() + ":" + l.unitPrice().toPlainString())
                .collect(Collectors.joining(";"));
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every Java platform", e);
        }
    }

    private static void validateCurrency(String currency, List<String> errors) {
        if (currency == null || currency.isBlank()) {
            errors.add("currency is required");
            return;
        }
        boolean knownCode = currency.matches("[A-Z]{3}")
                && Currency.getAvailableCurrencies().stream().anyMatch(c -> c.getCurrencyCode().equals(currency));
        if (!knownCode) {
            errors.add("currency must be an ISO 4217 code such as EUR, got '" + currency + "'");
        }
    }

    private static Optional<OrderLine> validateLine(int index, LineInput line, List<String> errors) {
        String prefix = "lines[" + index + "].";
        if (line == null) {
            errors.add("lines[" + index + "] must not be null");
            return Optional.empty();
        }
        int errorsBefore = errors.size();

        String sku = line.sku() == null ? null : line.sku().strip();
        if (sku == null || sku.isEmpty()) {
            errors.add(prefix + "sku is required");
        } else if (sku.length() > MAX_SKU_LENGTH) {
            errors.add(prefix + "sku must be at most " + MAX_SKU_LENGTH + " characters");
        }

        Integer quantity = line.quantity();
        if (quantity == null) {
            errors.add(prefix + "quantity is required");
        } else if (quantity < 1 || quantity > MAX_QUANTITY) {
            errors.add(prefix + "quantity must be between 1 and " + MAX_QUANTITY);
        }

        BigDecimal price = line.unitPrice();
        if (price == null) {
            errors.add(prefix + "unitPrice is required");
        } else if (price.signum() < 0) {
            errors.add(prefix + "unitPrice must not be negative");
        } else if (price.stripTrailingZeros().scale() > 2) {
            errors.add(prefix + "unitPrice must have at most 2 decimal places");
        }

        if (errors.size() > errorsBefore) {
            return Optional.empty();
        }
        return Optional.of(new OrderLine(index + 1, sku, quantity, price.setScale(2, RoundingMode.UNNECESSARY)));
    }

    private static BigDecimal total(List<OrderLine> lines) {
        return lines.stream().map(OrderLine::lineTotal).reduce(BigDecimal.ZERO, BigDecimal::add).setScale(2, RoundingMode.UNNECESSARY);
    }
}
