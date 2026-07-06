/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves a cold binlog start can cross a past DDL when the old schema is supplied via {@code
 * seedSchemas}, and that it still crashes without it (negative control).
 *
 * <p>Timeline: two rows are inserted with the 2-column schema, then {@code ALTER TABLE ... ADD
 * COLUMN} runs, then a third row is inserted with the 3-column schema. A cold binlog start ( {@code
 * earliest}) replays all of this from the beginning. Without a seed schema, the reader fetches the
 * CURRENT (3-column) schema via {@code SHOW CREATE TABLE} up front and then chokes decoding the
 * historical 2-column row images. Supplying the OLD (2-column) schema via {@code seedSchemas} lets
 * the reader start in sync with the historical rows and pick up the new column once Debezium's
 * in-flight schema history processes the ALTER.
 */
class MySqlSeedSchemaCrossDdlITCase extends MySqlSourceTestBase {

    // Empty init script: we create/populate the table by hand so we control the DDL timeline.
    private final UniqueDatabase db =
            new UniqueDatabase(MYSQL_CONTAINER, "seed_schema_it", "mysqluser", "mysqlpw");

    private String setUpCrossDdlTimeline() throws Exception {
        db.createAndInitialize();
        try (Connection conn = db.getJdbcConnection();
                Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE orders (id INT NOT NULL, amount INT, PRIMARY KEY (id))");
            st.execute("INSERT INTO orders VALUES (1, 100), (2, 200)"); // pre-DDL rows
            st.execute("ALTER TABLE orders ADD COLUMN note VARCHAR(64)"); // the DDL
            st.execute("INSERT INTO orders VALUES (3, 300, 'post')"); // post-DDL row
        }
        return "CREATE TABLE `orders` (`id` INT NOT NULL, `amount` INT, PRIMARY KEY (`id`))";
    }

    private MySqlSourceBuilder<String> baseBuilder() {
        return MySqlSource.<String>builder()
                .hostname(MYSQL_CONTAINER.getHost())
                .port(MYSQL_CONTAINER.getDatabasePort())
                .username(db.getUsername())
                .password(db.getPassword())
                .databaseList(db.getDatabaseName())
                .tableList(db.getDatabaseName() + ".orders")
                // Distinct range: other ITs in this module randomize within [5400, 5504).
                .serverId("8100-8103")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.earliest());
    }

    @Test
    void coldStartWithSeedSchemaCrossesDdl() throws Exception {
        String oldDdl = setUpCrossDdlTimeline();

        MySqlSource<String> source =
                baseBuilder()
                        .seedSchemas(
                                Collections.singletonMap(db.getDatabaseName() + ".orders", oldDdl))
                        .build();

        List<String> rows = collect(source, 3);

        // All three rows decoded; the post-DDL row carries the new column.
        assertThat(rows).hasSize(3);
        assertThat(rows).anyMatch(r -> r.contains("\"id\":3") && r.contains("post"));
    }

    @Test
    void coldStartWithoutSeedSchemaStillCrashes() throws Exception {
        setUpCrossDdlTimeline();
        MySqlSource<String> source = baseBuilder().build(); // no seedSchemas

        // executeAndCollect()'s iterator can stall rather than throw cleanly once the job has
        // failed, so drive this via executeAsync() + a real sink and assert on the job's own
        // terminal exception instead of an iterator timeout.
        assertThatThrownBy(() -> runToTerminalResult(source))
                .isInstanceOf(ExecutionException.class)
                .hasStackTraceContaining(
                        "internal schema representation is probably out of sync with real"
                                + " database schema");
    }

    private void runToTerminalResult(MySqlSource<String> source) throws Exception {
        StreamExecutionEnvironment env = buildEnv();
        DataStreamSource<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "seed-schema-it");
        stream.sinkTo(new DiscardingSink<>());
        JobClient jobClient = env.executeAsync("seed-schema-negative-control");
        jobClient.getJobExecutionResult().get();
    }

    private List<String> collect(MySqlSource<String> source, int expected) throws Exception {
        StreamExecutionEnvironment env = buildEnv();
        DataStreamSource<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "seed-schema-it");
        List<String> out = new ArrayList<>();
        try (CloseableIterator<String> it = stream.executeAndCollect()) {
            // NB: check the count BEFORE hasNext(). The source is an unbounded binlog stream, so
            // once the expected rows have been collected, calling hasNext() again would block
            // forever waiting for a row that never arrives.
            while (out.size() < expected && it.hasNext()) {
                out.add(it.next());
            }
        }
        return out;
    }

    private StreamExecutionEnvironment buildEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000L);
        // Without this, checkpointing implies infinite fixed-delay restarts, and the negative
        // control's job would retry the crash forever instead of surfacing a terminal failure.
        env.setRestartStrategy(RestartStrategies.noRestart());
        return env;
    }
}
