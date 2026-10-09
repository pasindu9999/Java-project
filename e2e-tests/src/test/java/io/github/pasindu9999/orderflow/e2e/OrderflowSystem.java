package io.github.pasindu9999.orderflow.e2e;

import io.github.pasindu9999.orderflow.inventory.InventoryServiceApplication;
import io.github.pasindu9999.orderflow.order.OrderServiceApplication;
import io.github.pasindu9999.orderflow.payment.PaymentServiceApplication;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/**
 * All three services in this JVM, each in its own Spring context, against one Postgres (three databases) and
 * one Kafka broker. That is the same topology as docker-compose, minus the network hops.
 *
 * <p>All three service jars put an {@code application.yml} at the classpath root, so only one would ever load.
 * Each context therefore reads its config from its own module's {@code src/main/resources/} via
 * {@code spring.config.location}: the real service config, not a copy. Command-line arguments then point the
 * datasource and Kafka at the containers. Flyway locations are already namespaced per service
 * ({@code db/migration/<service>}), so the migrations don't clash.
 */
final class OrderflowSystem implements AutoCloseable {

    private final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.6-alpine")
            .withDatabaseName("order_db")
            .withCopyFileToContainer(MountableFile.forClasspathResource("init-databases.sql"),
                    "/docker-entrypoint-initdb.d/init-databases.sql");
    // Same as compose: topics exist only if a service declares them.
    private final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.3.1")
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
    private final List<ConfigurableApplicationContext> contexts = new ArrayList<>();

    private ConfigurableApplicationContext order;
    private ConfigurableApplicationContext inventory;
    private ConfigurableApplicationContext payment;

    static OrderflowSystem start() {
        OrderflowSystem system = new OrderflowSystem();
        try {
            Startables.deepStart(system.postgres, system.kafka).join();
            // Any start order works: a listener whose topic doesn't exist yet waits until its producer declares it.
            system.inventory = system.boot(InventoryServiceApplication.class, "inventory-service", "inventory_db");
            system.payment = system.boot(PaymentServiceApplication.class, "payment-service", "payment_db");
            system.order = system.boot(OrderServiceApplication.class, "order-service", "order_db");
            return system;
        } catch (RuntimeException e) {
            system.close();
            throw e;
        }
    }

    String orderUrl() {
        return baseUrl(order);
    }

    String inventoryUrl() {
        return baseUrl(inventory);
    }

    String paymentUrl() {
        return baseUrl(payment);
    }

    /** Direct access to inventory_db, to create each test's own stock. */
    JdbcClient inventoryDb() {
        return inventory.getBean(JdbcClient.class);
    }

    @Override
    public void close() {
        for (int i = contexts.size() - 1; i >= 0; i--) {
            contexts.get(i).close();
        }
        kafka.stop();
        postgres.stop();
    }

    private ConfigurableApplicationContext boot(Class<?> application, String module, String database) {
        Path config = repositoryRoot().resolve(module).resolve("src/main/resources");
        String jdbcUrl = "jdbc:postgresql://%s:%d/%s".formatted(
                postgres.getHost(), postgres.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT), database);
        ConfigurableApplicationContext context = new SpringApplicationBuilder(application).run(
                "--spring.config.location=file:" + config.toString().replace('\\', '/') + "/",
                "--server.port=0",
                "--spring.main.banner-mode=off",
                // Logback is initialised once per JVM, by the first context, so every line would carry that
                // context's application name. Logger names (OrderSaga, ReservationService…) identify the service.
                "--logging.include-application-name=false",
                "--spring.datasource.url=" + jdbcUrl,
                "--spring.datasource.username=" + postgres.getUsername(),
                "--spring.datasource.password=" + postgres.getPassword(),
                "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers());
        contexts.add(context);
        return context;
    }

    private static String baseUrl(ConfigurableApplicationContext context) {
        return "http://localhost:" + context.getEnvironment().getRequiredProperty("local.server.port");
    }

    /** Maven runs tests from the module directory, IDEs sometimes from the root; walk up until it's found. */
    private static Path repositoryRoot() {
        for (Path dir = Path.of("").toAbsolutePath(); dir != null; dir = dir.getParent()) {
            if (Files.isDirectory(dir.resolve("order-service")) && Files.isRegularFile(dir.resolve("pom.xml"))) {
                return dir;
            }
        }
        throw new IllegalStateException("Run the e2e tests from inside the orderflow repository");
    }
}
