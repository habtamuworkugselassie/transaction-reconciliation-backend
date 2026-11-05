package com.reconciliation.infrastructure.persistence;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.DataSourceConnectionProvider;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;

/**
 * One PostgreSQL for the whole integration-test JVM, migrated once with the production
 * Flyway migrations.
 *
 * <p>Where it comes from, in order:
 * <ol>
 *   <li>{@code TEST_DATABASE_URL} (+ {@code TEST_DATABASE_USERNAME}/{@code TEST_DATABASE_PASSWORD},
 *       default postgres/postgres): an existing server, e.g. a CI service container or a local
 *       install. The schema is dropped and recreated, so never point it at a real database.</li>
 *   <li>otherwise a Testcontainers {@code postgres:16-alpine} container (needs Docker).</li>
 * </ol>
 * The jOOQ context and transaction manager are wired the same way Spring Boot wires them
 * (transaction-aware data source), so adapters behave exactly as in production.
 */
public final class PostgresSupport {

    private static volatile PostgresSupport instance;

    private final HikariDataSource dataSource;
    private final DSLContext db;
    private final PlatformTransactionManager txManager;
    private final String jdbcUrl;
    private final String username;
    private final String password;

    private PostgresSupport() {
        String url = System.getenv("TEST_DATABASE_URL");
        String user;
        String pass;
        if (url != null && !url.isBlank()) {
            user = env("TEST_DATABASE_USERNAME", "postgres");
            pass = env("TEST_DATABASE_PASSWORD", "postgres");
        } else {
            @SuppressWarnings("resource")
            PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("reconciliation_test")
                    .withUsername("postgres")
                    .withPassword("postgres")
                    .withReuse(false);
            container.start();
            Runtime.getRuntime().addShutdownHook(new Thread(container::stop));
            url = container.getJdbcUrl();
            user = container.getUsername();
            pass = container.getPassword();
        }
        this.jdbcUrl = url;
        this.username = user;
        this.password = pass;

        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(url);
        cfg.setUsername(user);
        cfg.setPassword(pass);
        cfg.setMaximumPoolSize(12);
        cfg.setPoolName("it-pool");
        this.dataSource = new HikariDataSource(cfg);

        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .cleanDisabled(false)
                .load()
                .clean();
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();

        DataSource txAware = new TransactionAwareDataSourceProxy(dataSource);
        this.db = DSL.using(new DataSourceConnectionProvider(txAware), SQLDialect.POSTGRES);
        this.txManager = new DataSourceTransactionManager(dataSource);
    }

    private static String env(String name, String fallback) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? fallback : v;
    }

    public static PostgresSupport get() {
        PostgresSupport s = instance;
        if (s == null) {
            synchronized (PostgresSupport.class) {
                s = instance;
                if (s == null) {
                    instance = s = new PostgresSupport();
                }
            }
        }
        return s;
    }

    public DSLContext db() {
        return db;
    }

    public DataSource dataSource() {
        return dataSource;
    }

    public TransactionTemplate transactionTemplate() {
        return new TransactionTemplate(txManager);
    }

    public String jdbcUrl() {
        return jdbcUrl;
    }

    public String username() {
        return username;
    }

    public String password() {
        return password;
    }

    /** Empties every table; the schema stays. Called before each test for isolation. */
    public void truncateAll() {
        db.execute("TRUNCATE TABLE import_attempts, import_summaries, rejected_records, transactions, imports RESTART IDENTITY CASCADE");
    }
}
