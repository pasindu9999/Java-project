package io.github.pasindu9999.orderflow.inventory.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.pasindu9999.orderflow.inventory.TestcontainersConfiguration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.client.RestClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
class StockApiIT {

    @Value("${local.server.port}")
    int port;

    @Autowired
    JdbcClient jdbc;

    @Test
    void shouldReturnStockLevels_whenSkuExists() {
        String sku = "MUG-" + UUID.randomUUID();
        jdbc.sql("INSERT INTO product_stock (sku, available, reserved) VALUES (:sku, 7, 2)").param("sku", sku).update();

        ResponseEntity<String> response = get(sku);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("\"available\":7", "\"reserved\":2");
    }

    @Test
    void shouldReturn404Problem_whenSkuIsUnknown() {
        ResponseEntity<String> response = get("NOPE-" + UUID.randomUUID());

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(response.getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
    }

    private ResponseEntity<String> get(String sku) {
        return RestClient.builder()
                .baseUrl("http://localhost:" + port)
                .defaultStatusHandler(status -> true, (request, response) -> { })
                .build()
                .get().uri("/stock/{sku}", sku)
                .retrieve()
                .toEntity(String.class);
    }
}
