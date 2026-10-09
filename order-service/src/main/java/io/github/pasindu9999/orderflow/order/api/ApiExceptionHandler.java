package io.github.pasindu9999.orderflow.order.api;

import io.github.pasindu9999.orderflow.order.app.IdempotencyKeyReusedException;
import io.github.pasindu9999.orderflow.order.app.OrderNotFoundException;
import io.github.pasindu9999.orderflow.order.domain.InvalidOrderException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * Every error is an RFC 9457 problem document. The base class already covers Spring MVC's own errors
 * (missing header, unreadable JSON, bad path variable) as 400s.
 */
@RestControllerAdvice
class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(InvalidOrderException.class)
    ProblemDetail invalidOrder(InvalidOrderException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "The order request is invalid.");
        problem.setTitle("Invalid order");
        problem.setProperty("errors", e.violations());
        return problem;
    }

    @ExceptionHandler(IdempotencyKeyReusedException.class)
    ProblemDetail idempotencyKeyReused(IdempotencyKeyReusedException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_CONTENT, e.getMessage());
        problem.setTitle("Idempotency key reused");
        return problem;
    }

    @ExceptionHandler(OrderNotFoundException.class)
    ProblemDetail orderNotFound(OrderNotFoundException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
        problem.setTitle("Order not found");
        return problem;
    }
}
