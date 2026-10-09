package io.github.pasindu9999.orderflow.payment.app;

import io.github.pasindu9999.orderflow.contracts.payment.PaymentCommand;
import io.github.pasindu9999.orderflow.contracts.payment.PaymentFailed;
import io.github.pasindu9999.orderflow.contracts.payment.PaymentRefunded;
import io.github.pasindu9999.orderflow.contracts.payment.PaymentSucceeded;
import io.github.pasindu9999.orderflow.contracts.payment.ProcessPayment;
import io.github.pasindu9999.orderflow.contracts.payment.RefundPayment;
import io.github.pasindu9999.orderflow.messaging.Envelope;
import io.github.pasindu9999.orderflow.messaging.NonRetryableMessageException;
import io.github.pasindu9999.orderflow.messaging.outbox.OutboxWriter;
import io.github.pasindu9999.orderflow.payment.config.PaymentProperties;
import io.github.pasindu9999.orderflow.payment.domain.DeclineReason;
import io.github.pasindu9999.orderflow.payment.domain.Payment;
import io.github.pasindu9999.orderflow.payment.domain.PaymentPolicy;
import io.github.pasindu9999.orderflow.payment.domain.PaymentPolicy.Approve;
import io.github.pasindu9999.orderflow.payment.domain.PaymentPolicy.Decline;
import io.github.pasindu9999.orderflow.payment.domain.PaymentPolicy.ProviderUnavailable;
import io.github.pasindu9999.orderflow.payment.domain.PaymentStatus;
import io.github.pasindu9999.orderflow.payment.persistence.PaymentRepository;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** Handles payment commands. Runs inside the inbox transaction, so the reply commits together with the payment row. */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository payments;
    private final OutboxWriter outbox;
    private final PaymentPolicy.Rules rules;
    /** Charge attempts per order, for the flaky-amount rule. In memory on purpose: it's only a simulator. */
    private final Map<UUID, Integer> attempts = new ConcurrentHashMap<>();

    public PaymentService(PaymentRepository payments, OutboxWriter outbox, PaymentProperties properties) {
        this.payments = payments;
        this.outbox = outbox;
        this.rules = properties.rules();
    }

    /** MANDATORY: only ever called from the idempotent inbox handler, which owns the transaction. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void handle(Envelope envelope) {
        switch (envelope.payloadAs(PaymentCommand.class)) {
            case ProcessPayment command -> charge(command, envelope.messageId());
            case RefundPayment command -> refund(command, envelope.messageId());
        }
    }

    private void charge(ProcessPayment command, UUID causationId) {
        validate(command);
        UUID orderId = command.orderId();
        Optional<Payment> existing = payments.findByOrderId(orderId);
        if (existing.isPresent()) {
            // A second command for an already charged order: the inbox only catches the same messageId.
            log.warn("Order {} already has a {} payment; ignoring ProcessPayment", orderId, existing.get().status());
            return;
        }

        int attempt = attempts.merge(orderId, 1, Integer::sum);
        switch (PaymentPolicy.decide(command.customerId(), command.amount(), attempt, rules)) {
            case Approve approve -> {
                Payment payment = newPayment(command, PaymentStatus.SUCCEEDED, null);
                payments.insert(payment);
                outbox.write(new PaymentSucceeded(orderId, payment.id(), payment.amount(), payment.currency()), causationId);
                log.info("Charged {} {} for order {} (attempt {})", command.amount(), command.currency(), orderId, attempt);
            }
            case Decline decline -> {
                payments.insert(newPayment(command, PaymentStatus.FAILED, decline.reason()));
                outbox.write(new PaymentFailed(orderId, failedReason(decline.reason())), causationId);
                log.info("Declined order {}: {}", orderId, decline.reason());
            }
            case ProviderUnavailable unavailable -> throw new TransientPaymentException(
                    "Payment provider unavailable for order " + orderId + " (attempt " + attempt + ")");
        }
        attempts.remove(orderId);
    }

    /**
     * Compensation for a charge that landed after the order was cancelled. Like any compensation it never fails
     * for business reasons: with nothing to refund, it does nothing.
     */
    private void refund(RefundPayment command, UUID causationId) {
        UUID orderId = command.orderId();
        Optional<Payment> payment = payments.lockByOrderId(orderId);
        if (payment.isEmpty() || payment.get().status() != PaymentStatus.SUCCEEDED) {
            log.warn("Nothing to refund for order {} (payment: {})", orderId, payment.map(p -> p.status().name()).orElse("none"));
            return;
        }
        payments.updateStatus(orderId, PaymentStatus.REFUNDED);
        outbox.write(new PaymentRefunded(orderId, payment.get().id()), causationId);
        log.info("Refunded {} {} for order {} ({})", payment.get().amount(), payment.get().currency(), orderId, command.reason());
    }

    private static void validate(ProcessPayment command) {
        if (command.customerId() == null || command.currency() == null
                || command.amount() == null || command.amount().signum() <= 0) {
            throw new NonRetryableMessageException("Invalid ProcessPayment for order " + command.orderId());
        }
    }

    private static Payment newPayment(ProcessPayment command, PaymentStatus status, DeclineReason reason) {
        return new Payment(UUID.randomUUID(), command.orderId(), command.customerId(), command.amount(),
                command.currency(), status, reason);
    }

    private static PaymentFailed.Reason failedReason(DeclineReason reason) {
        return switch (reason) {
            case DECLINED_LIMIT -> PaymentFailed.Reason.DECLINED_LIMIT;
            case DECLINED_BLOCKED_CUSTOMER -> PaymentFailed.Reason.DECLINED_BLOCKED_CUSTOMER;
        };
    }
}
