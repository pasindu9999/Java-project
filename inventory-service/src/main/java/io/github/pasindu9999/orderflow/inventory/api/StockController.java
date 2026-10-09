package io.github.pasindu9999.orderflow.inventory.api;

import io.github.pasindu9999.orderflow.inventory.persistence.StockRepository;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/** Read-only stock levels, for demos and tests. Stock only ever changes through Kafka commands. */
@RestController
class StockController {

    record StockResponse(String sku, int available, int reserved) {
    }

    private final StockRepository stock;

    StockController(StockRepository stock) {
        this.stock = stock;
    }

    @GetMapping("/stock/{sku}")
    ResponseEntity<?> getStock(@PathVariable String sku) {
        return stock.find(sku)
                .<ResponseEntity<?>>map(level -> ResponseEntity.ok(new StockResponse(level.sku(), level.available(), level.reserved())))
                .orElseGet(() -> ResponseEntity.of(
                        ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, "Unknown SKU " + sku)).build());
    }
}
