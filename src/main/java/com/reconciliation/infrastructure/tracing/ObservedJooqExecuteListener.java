package com.reconciliation.infrastructure.tracing;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import org.jooq.ExecuteContext;
import org.jooq.ExecuteListener;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * jOOQ {@link ExecuteListener} that wraps every statement execution in a Micrometer
 * observation, so each statement becomes a span ({@code jdbc.query}) nested under whatever
 * is being observed on the calling thread: a batch write, an HTTP request, a Kafka record.
 *
 * <p>The span is named {@code <verb> <table>} ("insert transactions", "select transactions",
 * "update imports"), which is what tells the bulk insert apart from the duplicate check in
 * a batch-write span. Verb and table are also low-cardinality tags, so the {@code jdbc.query}
 * timer stays bounded. The statement text is attached as a high-cardinality attribute only;
 * jOOQ renders it with {@code ?} placeholders, so bind values (record contents) are never in
 * a span.
 *
 * <p>The observation runs from {@code executeStart} to {@code end}, so it covers the round
 * trip and the fetch of any result, including {@code RETURNING} rows.
 *
 * <p>A statement is observed only when something is already being observed on the thread.
 * Statements issued outside any trace (the worker polling for jobs, the queue-depth gauge,
 * the retention sweep) would otherwise each become a one-span trace of their own, many
 * times a minute, and crowd the real ones out of the trace store.
 */
public final class ObservedJooqExecuteListener implements ExecuteListener {

    static final String OBSERVATION_NAME = "jdbc.query";
    static final int MAX_STATEMENT_LENGTH = 2000;

    private static final String KEY = ObservedJooqExecuteListener.class.getName() + ".observation";
    private static final Pattern VERB = Pattern.compile("^\\s*(with|select|insert|update|delete|merge|create|alter|drop|truncate)\\b",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern TABLE = Pattern.compile(
            "\\b(?:insert\\s+into|update|delete\\s+from|from|into)\\s+(?:\"?[\\w$]+\"?\\.)*\"?([\\w$]+)\"?",
            Pattern.CASE_INSENSITIVE);

    private final ObservationRegistry registry;

    public ObservedJooqExecuteListener(ObservationRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void executeStart(ExecuteContext ctx) {
        if (registry.isNoop() || registry.getCurrentObservation() == null) {
            return;
        }
        String sql = statement(ctx);
        String verb = verb(sql);
        String table = table(sql, verb);
        Observation observation = Observation.createNotStarted(OBSERVATION_NAME, registry)
                .contextualName(verb + " " + table)
                .lowCardinalityKeyValue("db.system", "postgresql")
                .lowCardinalityKeyValue("db.operation", verb)
                .lowCardinalityKeyValue("db.sql.table", table)
                .highCardinalityKeyValue("db.statement", truncate(sql))
                .start();
        ctx.data(KEY, observation);
    }

    @Override
    public void exception(ExecuteContext ctx) {
        Observation observation = (Observation) ctx.data(KEY);
        if (observation != null && ctx.exception() != null) {
            observation.error(ctx.exception());
        }
    }

    @Override
    public void end(ExecuteContext ctx) {
        Observation observation = (Observation) ctx.data(KEY);
        if (observation != null) {
            ctx.data().remove(KEY);
            observation.stop();
        }
    }

    private static String statement(ExecuteContext ctx) {
        String sql = ctx.sql();
        if (sql == null) {
            String[] batch = ctx.batchSQL();
            sql = batch != null && batch.length > 0 && batch[0] != null ? batch[0] : "";
        }
        return sql;
    }

    static String verb(String sql) {
        Matcher m = VERB.matcher(sql);
        if (!m.find()) {
            return "query";
        }
        String verb = m.group(1).toLowerCase(Locale.ROOT);
        return "with".equals(verb) ? "select" : verb;
    }

    static String table(String sql, String verb) {
        // For an INSERT ... SELECT the first FROM belongs to the subselect; the target comes first anyway
        // because the pattern scans left to right and "insert into" precedes it.
        Matcher m = TABLE.matcher(sql);
        if (!m.find()) {
            return "unknown";
        }
        return m.group(1).toLowerCase(Locale.ROOT);
    }

    private static String truncate(String sql) {
        return sql.length() <= MAX_STATEMENT_LENGTH ? sql : sql.substring(0, MAX_STATEMENT_LENGTH) + "...";
    }
}
