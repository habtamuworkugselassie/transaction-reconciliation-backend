package com.reconciliation.infrastructure.tracing;

import io.micrometer.common.KeyValue;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultExecuteListenerProvider;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockResult;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ObservedJooqExecuteListenerTest {

    /** Records every observation the listener stops, with the parent it was started under. */
    private static final class Recording implements ObservationHandler<Observation.Context> {
        final List<Observation.Context> stopped = new ArrayList<>();

        @Override
        public void onStop(Observation.Context context) {
            stopped.add(context);
        }

        @Override
        public boolean supportsContext(Observation.Context context) {
            return true;
        }
    }

    private final Recording recording = new Recording();
    private final ObservationRegistry registry = ObservationRegistry.create();

    {
        registry.observationConfig().observationHandler(recording);
    }

    private DSLContext db(MockDataProvider provider) {
        return DSL.using(new DefaultConfiguration()
                .set(new MockConnection(provider))
                .set(SQLDialect.POSTGRES)
                .set(new DefaultExecuteListenerProvider(new ObservedJooqExecuteListener(registry))));
    }

    private static MockResult[] noRows(org.jooq.tools.jdbc.MockExecuteContext ctx) {
        return new MockResult[]{new MockResult(0, DSL.using(SQLDialect.POSTGRES).newResult())};
    }

    /** Runs {@code work} inside a parent observation, as a batch write or HTTP request would. */
    private Observation traced(Runnable work) {
        Observation parent = Observation.start("import.batch.write", registry);
        try (Observation.Scope ignored = parent.openScope()) {
            work.run();
        } finally {
            parent.stop();
        }
        return parent;
    }

    private List<Observation.Context> statements() {
        return recording.stopped.stream().filter(c -> ObservedJooqExecuteListener.OBSERVATION_NAME.equals(c.getName())).toList();
    }

    private static String tag(Observation.Context ctx, String key) {
        return ctx.getAllKeyValues().stream().filter(kv -> kv.getKey().equals(key))
                .map(KeyValue::getValue).findFirst().orElse(null);
    }

    @Test
    void namesTheSpanAfterVerbAndTargetTable() {
        DSLContext db = db(ObservedJooqExecuteListenerTest::noRows);

        traced(() -> {
            db.execute("insert into \"transactions\" (\"import_id\") select ? from unnest(?) as u(x)", "i", new String[]{"a"});
            db.fetch("select \"transactions\".\"transaction_id\" from \"transactions\" where \"provider_id\" = ?", "p");
            db.execute("update \"imports\" set \"processed_count\" = ? where \"id\" = ?", 1, "x");
            db.fetch("select * from \"imports\" where \"id\" = ? for update", "x");
        });

        assertThat(statements()).extracting(Observation.Context::getContextualName)
                .containsExactly("insert transactions", "select transactions", "update imports", "select imports");
        Observation.Context insert = statements().get(0);
        assertThat(insert.getName()).isEqualTo("jdbc.query");
        assertThat(tag(insert, "db.operation")).isEqualTo("insert");
        assertThat(tag(insert, "db.sql.table")).isEqualTo("transactions");
        assertThat(tag(insert, "db.system")).isEqualTo("postgresql");
        // statement text carries placeholders, never bind values
        assertThat(tag(insert, "db.statement")).contains("insert into \"transactions\"").doesNotContain("'i'");
        assertThat(insert.getError()).isNull();
    }

    @Test
    void nestsUnderTheObservationCurrentOnTheCallingThread() {
        DSLContext db = db(ObservedJooqExecuteListenerTest::noRows);

        Observation parent = traced(() -> db.fetch("select 1 from \"imports\""));

        Observation.Context child = statements().get(0);
        assertThat(child.getContextualName()).isEqualTo("select imports");
        assertThat(child.getParentObservation()).isSameAs(parent);
    }

    @Test
    void neverStartsATraceOfItsOwn() {
        DSLContext db = db(ObservedJooqExecuteListenerTest::noRows);

        // no observation is current: a poll for work, a gauge sample, a retention sweep
        db.fetch("select 1 from \"imports\"");

        assertThat(recording.stopped).isEmpty();
    }

    @Test
    void recordsTheFailureAndStillStopsTheObservation() {
        DSLContext db = db(ctx -> {
            throw new SQLException("boom");
        });

        traced(() -> assertThatThrownBy(() -> db.execute("delete from \"imports\" where \"id\" = ?", "x"))
                .isInstanceOf(DataAccessException.class));

        assertThat(statements()).hasSize(1);
        Observation.Context failed = statements().get(0);
        assertThat(failed.getContextualName()).isEqualTo("delete imports");
        assertThat(failed.getError()).isNotNull();
    }

    @Test
    void parsesVerbsAndTablesDefensively() {
        assertThat(ObservedJooqExecuteListener.verb("WITH x AS (select 1) select * from x")).isEqualTo("select");
        assertThat(ObservedJooqExecuteListener.verb("  Delete from \"a\"")).isEqualTo("delete");
        assertThat(ObservedJooqExecuteListener.verb("explain select 1")).isEqualTo("query");
        assertThat(ObservedJooqExecuteListener.table("update \"public\".\"imports\" set x = 1", "update")).isEqualTo("imports");
        assertThat(ObservedJooqExecuteListener.table("select 1", "select")).isEqualTo("unknown");
        assertThat(ObservedJooqExecuteListener.table("", "query")).isEqualTo("unknown");
    }

    @Test
    void isInertWhenTheRegistryIsNoop() {
        DSLContext db = DSL.using(new DefaultConfiguration()
                .set(new MockConnection(ObservedJooqExecuteListenerTest::noRows))
                .set(SQLDialect.POSTGRES)
                .set(new DefaultExecuteListenerProvider(new ObservedJooqExecuteListener(ObservationRegistry.NOOP))));

        traced(() -> db.fetch("select 1 from \"imports\""));

        assertThat(statements()).isEmpty();
    }
}
