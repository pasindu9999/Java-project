package io.github.pasindu9999.orderflow.payment;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.web.client.RestClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
class PaymentServiceApplicationIT {

    record Health(String status) {}

    @Value("${local.server.port}")
    int port;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    KafkaAdmin kafkaAdmin;

    @Test
    void shouldReportHealthUp_whenStartedAgainstRealInfrastructure() {
        Health health = RestClient.create("http://localhost:" + port)
                .get().uri("/actuator/health")
                .retrieve()
                .body(Health.class);

        assertThat(health.status()).isEqualTo("UP");
    }

    @Test
    void shouldCreateMessagingTables_whenFlywayMigrationsRun() {
        var tables = jdbc.sql("""
                        SELECT table_name FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_name IN ('outbox', 'processed_message')
                        """)
                .query(String.class)
                .list();

        assertThat(tables).containsExactlyInAnyOrder("outbox", "processed_message");
    }

    @Test
    void shouldReachKafkaBroker_whenContainerIsRunning() throws Exception {
        try (var admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            var nodes = admin.describeCluster().nodes().get(10, TimeUnit.SECONDS);

            assertThat(nodes).isNotEmpty();
        }
    }
}
