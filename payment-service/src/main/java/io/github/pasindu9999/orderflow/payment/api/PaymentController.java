package io.github.pasindu9999.orderflow.payment.api;

import io.github.pasindu9999.orderflow.payment.domain.Payment;
import io.github.pasindu9999.orderflow.payment.persistence.PaymentRepository;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Read-only payment records, for demos and tests. Payments only ever change through Kafka commands. */
@RestController
class PaymentController {

    /** Money is a decimal string, matching the Kafka wire format. */
    record PaymentResponse(UUID paymentId, UUID orderId, UUID customerId, String amount, String currency,
                           String status, String failureReason) {

        static PaymentResponse from(Payment p) {
            return new PaymentResponse(p.id(), p.orderId(), p.customerId(), p.amount().toPlainString(), p.currency(),
                    p.status().name(), p.failureReason() == null ? null : p.failureReason().name());
        }
    }

    private final PaymentRepository payments;

    PaymentController(PaymentRepository payments) {
        this.payments = payments;
    }

    @GetMapping("/payments")
    ResponseEntity<?> findByOrder(@RequestParam UUID orderId) {
        return payments.findByOrderId(orderId)
                .<ResponseEntity<?>>map(payment -> ResponseEntity.ok(PaymentResponse.from(payment)))
                .orElseGet(() -> ResponseEntity.of(
                        ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, "No payment for order " + orderId)).build());
    }
}
