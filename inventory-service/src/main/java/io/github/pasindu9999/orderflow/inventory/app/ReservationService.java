package io.github.pasindu9999.orderflow.inventory.app;

import io.github.pasindu9999.orderflow.contracts.inventory.InventoryCommand;
import io.github.pasindu9999.orderflow.contracts.inventory.InventoryRejected;
import io.github.pasindu9999.orderflow.contracts.inventory.InventoryReserved;
import io.github.pasindu9999.orderflow.contracts.inventory.ReleaseInventory;
import io.github.pasindu9999.orderflow.contracts.inventory.ReserveInventory;
import io.github.pasindu9999.orderflow.inventory.domain.ReservationLine;
import io.github.pasindu9999.orderflow.inventory.domain.ReservationPolicy;
import io.github.pasindu9999.orderflow.inventory.domain.ReservationPolicy.Accept;
import io.github.pasindu9999.orderflow.inventory.domain.ReservationPolicy.Reject;
import io.github.pasindu9999.orderflow.inventory.domain.ReservationStatus;
import io.github.pasindu9999.orderflow.inventory.domain.StockLevel;
import io.github.pasindu9999.orderflow.inventory.persistence.ReservationRepository;
import io.github.pasindu9999.orderflow.inventory.persistence.StockRepository;
import io.github.pasindu9999.orderflow.messaging.Envelope;
import io.github.pasindu9999.orderflow.messaging.NonRetryableMessageException;
import io.github.pasindu9999.orderflow.messaging.outbox.OutboxWriter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** Handles inventory commands. Runs inside the inbox transaction, so the reply commits together with the stock change. */
@Service
public class ReservationService {

    private static final Logger log = LoggerFactory.getLogger(ReservationService.class);

    private final StockRepository stock;
    private final ReservationRepository reservations;
    private final OutboxWriter outbox;

    public ReservationService(StockRepository stock, ReservationRepository reservations, OutboxWriter outbox) {
        this.stock = stock;
        this.reservations = reservations;
        this.outbox = outbox;
    }

    /** MANDATORY: only ever called from the idempotent inbox handler, which owns the transaction. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void handle(Envelope envelope) {
        switch (envelope.payloadAs(InventoryCommand.class)) {
            case ReserveInventory command -> reserve(command, envelope.messageId());
            case ReleaseInventory command -> release(command);
        }
    }

    /**
     * Compensation (ARCHITECTURE §6.3): never fails for business reasons, and needs no reply. The reservation row
     * is locked first, so a release can't interleave with anything else touching the same order.
     */
    private void release(ReleaseInventory command) {
        UUID orderId = command.orderId();
        Optional<ReservationStatus> status = reservations.lockStatus(orderId);
        if (status.isEmpty()) {
            // Tombstone: a ReserveInventory that arrives later (e.g. replayed from the DLT) finds a row and is ignored.
            // If a reserve for this order commits first, this insert fails and the retried release finds RESERVED.
            reservations.insert(orderId, ReservationStatus.RELEASED, List.of());
            log.info("Released order {} before any reservation ({}); tombstone written", orderId, command.reason());
            return;
        }
        if (status.get() != ReservationStatus.RESERVED) {
            log.info("Order {} is already {}; nothing to release ({})", orderId, status.get(), command.reason());
            return;
        }

        List<ReservationLine> lines = reservations.findLines(orderId);
        stock.lockForUpdate(lines.stream().map(ReservationLine::sku).toList()); // same SKU order as reserve
        lines.forEach(line -> stock.release(line.sku(), line.quantity()));
        reservations.updateStatus(orderId, ReservationStatus.RELEASED);
        log.info("Released {} line(s) for order {} ({})", lines.size(), orderId, command.reason());
    }

    private void reserve(ReserveInventory command, UUID causationId) {
        UUID orderId = command.orderId();
        Optional<ReservationStatus> existing = reservations.findStatus(orderId);
        if (existing.isPresent()) {
            // A second command for an order that already has an outcome: the inbox only catches the same messageId.
            log.warn("Order {} already has a {} reservation; ignoring ReserveInventory", orderId, existing.get());
            return;
        }

        List<ReservationLine> lines = normalise(command);
        Map<String, StockLevel> locked = stock.lockForUpdate(lines.stream().map(ReservationLine::sku).toList());

        switch (ReservationPolicy.decide(lines, locked)) {
            case Accept accept -> {
                accept.lines().forEach(line -> stock.reserve(line.sku(), line.quantity()));
                reservations.insert(orderId, ReservationStatus.RESERVED, accept.lines());
                outbox.write(new InventoryReserved(orderId), causationId);
                log.info("Reserved {} line(s) for order {}", accept.lines().size(), orderId);
            }
            case Reject reject -> {
                reservations.insert(orderId, ReservationStatus.REJECTED, List.of());
                outbox.write(rejected(orderId, reject), causationId);
                log.info("Rejected order {}: {} {}", orderId, reject.reason(), reject.shortages());
            }
        }
    }

    private static List<ReservationLine> normalise(ReserveInventory command) {
        try {
            List<ReservationLine> requested = command.lines() == null ? null : command.lines().stream()
                    .map(line -> line == null ? null : new ReservationLine(line.sku(), line.quantity()))
                    .toList();
            return ReservationPolicy.normalise(requested);
        } catch (IllegalArgumentException e) {
            throw new NonRetryableMessageException("Invalid ReserveInventory for order " + command.orderId() + ": " + e.getMessage());
        }
    }

    private static InventoryRejected rejected(UUID orderId, Reject reject) {
        InventoryRejected.Reason reason = switch (reject.reason()) {
            case OUT_OF_STOCK -> InventoryRejected.Reason.OUT_OF_STOCK;
            case UNKNOWN_SKU -> InventoryRejected.Reason.UNKNOWN_SKU;
        };
        return new InventoryRejected(orderId, reason, reject.shortages().stream()
                .map(s -> new InventoryRejected.Shortage(s.sku(), s.requested(), s.available()))
                .toList());
    }
}
