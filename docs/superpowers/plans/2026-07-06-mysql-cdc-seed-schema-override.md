# MySQL CDC Custom Seed-Schema Override — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an operator supply the old `CREATE TABLE` DDL so a cold-start binlog read (`earliest`/`timestamp`/`specific-offset`, no Flink state) seeds its decoding schema from that DDL instead of live `SHOW CREATE TABLE`, enabling cross-DDL reprocessing without a snapshot.

**Architecture:** Add an optional `Map<String,String>` (`"db.table"` → old `CREATE TABLE` DDL) threaded from `MySqlSourceBuilder` through `MySqlSourceConfigFactory` into `MySqlSourceConfig`. On a cold binlog start, `TableDiscoveryUtils` consults it per table: overridden tables are built from the supplied DDL via a new `MySqlSchema.parseTableSchema`; all others fall back to `SHOW CREATE TABLE`. Streaming DDL-processing (forward `ALTER` pickup) and all snapshot/restore paths are unchanged.

**Tech Stack:** Java 8, Apache Flink CDC, Debezium (embedded), JUnit 5, Mockito, Testcontainers (MySQL).

## Global Constraints

- Module root: `flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc`.
- Package base: `org.apache.flink.cdc.connectors.mysql`.
- Formatting enforced by Spotless (Google Java Format) + Checkstyle — run `mvn spotless:apply` before every commit.
- Default behavior must be byte-for-byte unchanged when `seedSchemas` is empty (the new field defaults to an empty map).
- Logging: no `INFO`/`DEBUG`. Guardrail messages are `WARN` (they carry operational-alert value).
- Delivery: internal fork; keep changes minimal and localized.
- Build a single module: `mvn -pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc -am ...`.
- Base branch: `tejanshrana/flink-cdc @ release-3.5-custom-2` (the internal fork). Line numbers below were derived from `master`; locate edits by the named symbol, not the line number.
- Conventional commits with scope `mysql-cdc` (Jira ID omitted for now). Co-author line required on every commit.

All paths below are relative to the module root unless absolute.

---

### Task 1: Thread `seedSchemas` config through builder → factory → config

**Files:**
- Modify: `src/main/java/org/apache/flink/cdc/connectors/mysql/source/config/MySqlSourceConfig.java`
- Modify: `src/main/java/org/apache/flink/cdc/connectors/mysql/source/config/MySqlSourceConfigFactory.java`
- Modify: `src/main/java/org/apache/flink/cdc/connectors/mysql/source/MySqlSourceBuilder.java`
- Test: `src/test/java/org/apache/flink/cdc/connectors/mysql/source/config/MySqlSourceConfigSeedSchemaTest.java`

**Interfaces:**
- Produces:
  - `MySqlSourceConfig.getSeedSchemas() : Map<String,String>` (never null; empty by default)
  - `MySqlSourceConfigFactory.seedSchemas(Map<String,String>) : MySqlSourceConfigFactory`
  - `MySqlSourceBuilder<T>.seedSchemas(Map<String,String>) : MySqlSourceBuilder<T>`

- [ ] **Step 1: Write the failing test**

Create `src/test/java/org/apache/flink/cdc/connectors/mysql/source/config/MySqlSourceConfigSeedSchemaTest.java`:

```java
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

package org.apache.flink.cdc.connectors.mysql.source.config;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that the seed-schema override threads through the config plumbing. */
class MySqlSourceConfigSeedSchemaTest {

    private MySqlSourceConfigFactory baseFactory() {
        return new MySqlSourceConfigFactory()
                .hostname("localhost")
                .port(3306)
                .username("user")
                .password("pass")
                .databaseList("mydb")
                .tableList("mydb.orders");
    }

    @Test
    void seedSchemasDefaultsToEmpty() {
        MySqlSourceConfig config = baseFactory().createConfig(0);
        assertThat(config.getSeedSchemas()).isNotNull().isEmpty();
    }

    @Test
    void seedSchemasArePassedThrough() {
        Map<String, String> seed =
                Collections.singletonMap(
                        "mydb.orders", "CREATE TABLE `orders` (`id` INT PRIMARY KEY)");
        MySqlSourceConfig config = baseFactory().seedSchemas(seed).createConfig(0);
        assertThat(config.getSeedSchemas()).containsEntry("mydb.orders", seed.get("mydb.orders"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc test -Dtest=MySqlSourceConfigSeedSchemaTest`
Expected: COMPILE FAILURE — `seedSchemas(...)` and `getSeedSchemas()` do not exist.

- [ ] **Step 3: Add field + constructor param + getter on `MySqlSourceConfig`**

In `MySqlSourceConfig.java`, add the field next to `chunkKeyColumns` (after line 70):

```java
    private final Map<ObjectPath, String> chunkKeyColumns;
    private final Map<String, String> seedSchemas;
```

Add a constructor parameter as the **last** parameter (after `boolean assignUnboundedChunkFirst`):

```java
            boolean useLegacyJsonFormat,
            boolean assignUnboundedChunkFirst,
            Map<String, String> seedSchemas) {
```

Assign it at the end of the constructor body (after `this.assignUnboundedChunkFirst = assignUnboundedChunkFirst;`):

```java
        this.assignUnboundedChunkFirst = assignUnboundedChunkFirst;
        this.seedSchemas = seedSchemas == null ? new HashMap<>() : seedSchemas;
```

Add the getter next to `getChunkKeyColumns()` (after line 292):

```java
    public Map<ObjectPath, String> getChunkKeyColumns() {
        return chunkKeyColumns;
    }

    /**
     * Old {@code CREATE TABLE} DDL per {@code "db.table"} used to seed the decoding schema on a
     * cold binlog start. Empty by default; see {@link
     * org.apache.flink.cdc.connectors.mysql.source.utils.TableDiscoveryUtils}.
     */
    public Map<String, String> getSeedSchemas() {
        return seedSchemas;
    }
```

Ensure `java.util.HashMap` is imported (it may already be). If not, add `import java.util.HashMap;`.

- [ ] **Step 4: Add field + setter on `MySqlSourceConfigFactory` and pass it to the constructor**

In `MySqlSourceConfigFactory.java`, add the field next to `chunkKeyColumns` (after line 75):

```java
    private Map<ObjectPath, String> chunkKeyColumns = new HashMap<>();
    private Map<String, String> seedSchemas = new HashMap<>();
```

Add a setter next to `chunkKeyColumn(Map<ObjectPath, String>)`:

```java
    /**
     * Old {@code CREATE TABLE} DDL keyed by {@code "db.table"}, used to seed the decoding schema on
     * a cold binlog start instead of live {@code SHOW CREATE TABLE}. Optional.
     */
    public MySqlSourceConfigFactory seedSchemas(Map<String, String> seedSchemas) {
        if (seedSchemas != null) {
            this.seedSchemas.putAll(seedSchemas);
        }
        return this;
    }
```

In BOTH `new MySqlSourceConfig(...)` argument lists (there is one, at the tail of `createConfig`), add `seedSchemas` as the final argument, after `assignUnboundedChunkFirst`:

```java
                useLegacyJsonFormat,
                assignUnboundedChunkFirst,
                seedSchemas);
```

Add the startup-mode guardrail WARN inside `createConfig(int, String)`, immediately before the `return new MySqlSourceConfig(...)`:

```java
        if (!seedSchemas.isEmpty() && !isPureBinlogStartup(startupOptions)) {
            LOG.warn(
                    "seedSchemas was set ({} entries) but startup mode is {}; the seed-schema "
                            + "override only applies to a cold binlog start (earliest/timestamp/"
                            + "specific-offset with no state) and will be ignored here.",
                    seedSchemas.size(),
                    startupOptions.startupMode);
        }
```

Add a private helper at the bottom of the class:

```java
    private static boolean isPureBinlogStartup(StartupOptions startupOptions) {
        switch (startupOptions.startupMode) {
            case EARLIEST_OFFSET:
            case TIMESTAMP:
            case SPECIFIC_OFFSETS:
                return true;
            default:
                return false;
        }
    }
```

If `MySqlSourceConfigFactory` has no logger, add near the top of the class body:

```java
    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(MySqlSourceConfigFactory.class);
```

> Verify the enum constant names against `org.apache.flink.cdc.connectors.mysql.table.StartupMode` before compiling (they are `INITIAL`, `EARLIEST_OFFSET`, `LATEST_OFFSET`, `SPECIFIC_OFFSETS`, `TIMESTAMP`). Adjust the `case` labels to match exactly. `StartupOptions.startupMode` is a public field on `StartupOptions`.

- [ ] **Step 5: Add delegating method on `MySqlSourceBuilder`**

In `MySqlSourceBuilder.java`, add next to `chunkKeyColumn(...)` (after line 130):

```java
    /**
     * Old {@code CREATE TABLE} DDL keyed by {@code "db.table"}, used to seed the decoding schema on
     * a cold binlog start (earliest/timestamp/specific-offset with no Flink state) instead of live
     * {@code SHOW CREATE TABLE}. The DDL must describe the schema in effect *at the start offset*.
     */
    public MySqlSourceBuilder<T> seedSchemas(Map<String, String> seedSchemas) {
        this.configFactory.seedSchemas(seedSchemas);
        return this;
    }
```

Ensure `import java.util.Map;` is present in `MySqlSourceBuilder.java`.

- [ ] **Step 6: Format, run test to verify it passes**

Run: `mvn -q -pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc spotless:apply && mvn -q -pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc -am test -Dtest=MySqlSourceConfigSeedSchemaTest`
Expected: PASS (both tests).

- [ ] **Step 7: Commit**

```bash
git add flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc/src/main/java/org/apache/flink/cdc/connectors/mysql/source/config/MySqlSourceConfig.java \
        flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc/src/main/java/org/apache/flink/cdc/connectors/mysql/source/config/MySqlSourceConfigFactory.java \
        flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc/src/main/java/org/apache/flink/cdc/connectors/mysql/source/MySqlSourceBuilder.java \
        flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc/src/test/java/org/apache/flink/cdc/connectors/mysql/source/config/MySqlSourceConfigSeedSchemaTest.java
git commit -m "feat(mysql-cdc): add seedSchemas option plumbing (builder/factory/config)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: `MySqlSchema.parseTableSchema` — build a `TableChange` from operator DDL

**Files:**
- Modify: `src/main/java/org/apache/flink/cdc/connectors/mysql/schema/MySqlSchema.java`
- Test: `src/test/java/org/apache/flink/cdc/connectors/mysql/schema/MySqlSchemaParseDdlTest.java`

**Interfaces:**
- Consumes: `MySqlSourceConfig` (Task 1) for construction.
- Produces: `MySqlSchema.parseTableSchema(MySqlPartition partition, TableId tableId, String createTableDdl) : TableChanges.TableChange`

- [ ] **Step 1: Write the failing test**

Create `src/test/java/org/apache/flink/cdc/connectors/mysql/schema/MySqlSchemaParseDdlTest.java`:

```java
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

package org.apache.flink.cdc.connectors.mysql.schema;

import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;

import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests offline DDL parsing into a Debezium {@link TableChange}. */
class MySqlSchemaParseDdlTest {

    @Test
    void parsesCreateTableDdlIntoTableChange() {
        MySqlSourceConfig config =
                new MySqlSourceConfigFactory()
                        .hostname("localhost")
                        .port(3306)
                        .username("user")
                        .password("pass")
                        .databaseList("mydb")
                        .tableList("mydb.orders")
                        .createConfig(0);

        TableId tableId = new TableId("mydb", null, "orders");
        MySqlPartition partition =
                new MySqlPartition(config.getMySqlConnectorConfig().getLogicalName());

        String oldDdl =
                "CREATE TABLE `orders` ("
                        + "`id` INT NOT NULL, "
                        + "`amount` INT, "
                        + "PRIMARY KEY (`id`))";

        try (MySqlSchema schema = new MySqlSchema(config, false)) {
            TableChange change = schema.parseTableSchema(partition, tableId, oldDdl);

            assertThat(change).isNotNull();
            assertThat(change.getTable().columns())
                    .extracting(io.debezium.relational.Column::name)
                    .containsExactly("id", "amount");
            assertThat(change.getTable().primaryKeyColumnNames()).containsExactly("id");
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc test -Dtest=MySqlSchemaParseDdlTest`
Expected: COMPILE FAILURE — `parseTableSchema(...)` does not exist.

- [ ] **Step 3: Add the public method**

In `MySqlSchema.java`, add after `getTableSchema(...)` (after line 71):

```java
    /**
     * Builds a {@link TableChange} for {@code tableId} directly from a supplied {@code CREATE
     * TABLE} DDL, bypassing {@code SHOW CREATE TABLE}. Used to seed the schema on a cold binlog
     * start when the current DB schema no longer matches the rows at the start offset.
     */
    public TableChange parseTableSchema(
            MySqlPartition partition, TableId tableId, String createTableDdl) {
        final Map<TableId, TableChange> tableChangeMap = new HashMap<>();
        parseSchemaByDdl(partition, createTableDdl, tableId, tableChangeMap);
        TableChange tableChange = tableChangeMap.get(tableId);
        if (tableChange == null) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Failed to parse seed schema DDL for table %s. DDL was: %s",
                            tableId, createTableDdl));
        }
        return tableChange;
    }
```

(`MySqlPartition`, `TableId`, `TableChange`, `Map`, `HashMap`, and `FlinkRuntimeException` are already imported in this file.)

- [ ] **Step 4: Format, run test to verify it passes**

Run: `mvn -q -pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc spotless:apply && mvn -q -pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc -am test -Dtest=MySqlSchemaParseDdlTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc/src/main/java/org/apache/flink/cdc/connectors/mysql/schema/MySqlSchema.java \
        flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc/src/test/java/org/apache/flink/cdc/connectors/mysql/schema/MySqlSchemaParseDdlTest.java
git commit -m "feat(mysql-cdc): parse operator-supplied CREATE TABLE DDL into TableChange

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: `TableDiscoveryUtils` — per-table seed selection with fallback

**Files:**
- Modify: `src/main/java/org/apache/flink/cdc/connectors/mysql/source/utils/TableDiscoveryUtils.java`
- Test: `src/test/java/org/apache/flink/cdc/connectors/mysql/source/utils/TableDiscoveryUtilsSeedSchemaTest.java`

**Interfaces:**
- Consumes: `MySqlSourceConfig.getSeedSchemas()` (Task 1), `MySqlSchema.parseTableSchema(...)` (Task 2).
- Produces: unchanged public signature
  `discoverSchemaForCapturedTables(MySqlPartition, List<TableId>, MySqlSourceConfig, MySqlConnection) : Map<TableId, TableChange>` — now honours the seed override per table.

- [ ] **Step 1: Write the failing test**

Create `src/test/java/org/apache/flink/cdc/connectors/mysql/source/utils/TableDiscoveryUtilsSeedSchemaTest.java`:

```java
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

package org.apache.flink.cdc.connectors.mysql.source.utils;

import org.apache.flink.cdc.connectors.mysql.debezium.reader.MySqlConnection;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;

import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Tests that seed-schema overrides bypass JDBC and non-overridden tables fall back. */
class TableDiscoveryUtilsSeedSchemaTest {

    private MySqlSourceConfig configWithSeed(Map<String, String> seed) {
        return new MySqlSourceConfigFactory()
                .hostname("localhost")
                .port(3306)
                .username("user")
                .password("pass")
                .databaseList("mydb")
                .tableList("mydb.orders")
                .seedSchemas(seed)
                .createConfig(0);
    }

    @Test
    void overriddenTableIsBuiltFromDdlWithoutTouchingJdbc() throws Exception {
        TableId orders = new TableId("mydb", null, "orders");
        MySqlSourceConfig config =
                configWithSeed(
                        Collections.singletonMap(
                                "mydb.orders",
                                "CREATE TABLE `orders` (`id` INT NOT NULL, PRIMARY KEY (`id`))"));

        MySqlConnection jdbc = mock(MySqlConnection.class);
        when(jdbc.isTableIdCaseSensitive()).thenReturn(false);
        MySqlPartition partition =
                new MySqlPartition(config.getMySqlConnectorConfig().getLogicalName());

        Map<TableId, TableChange> schemas =
                TableDiscoveryUtils.discoverSchemaForCapturedTables(
                        partition, Collections.singletonList(orders), config, jdbc);

        assertThat(schemas).containsKey(orders);
        assertThat(schemas.get(orders).getTable().columns())
                .extracting(io.debezium.relational.Column::name)
                .containsExactly("id");
        // The whole point: no SHOW CREATE TABLE / DESC query was issued for the overridden table.
        verifyNoInteractions(jdbc);
    }
}
```

> Note: `verifyNoInteractions(jdbc)` asserts the overridden path never queries. `MySqlSchema`'s constructor takes `jdbc.isTableIdCaseSensitive()`; to keep it interaction-free for the override path, pass the boolean explicitly at the call site (see Step 3) instead of calling it inside the loop. If Mockito flags the stubbed `isTableIdCaseSensitive()`, switch the assertion to `verify(jdbc, never()).query(anyString(), any())` and remove the `when(...)` stub.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc test -Dtest=TableDiscoveryUtilsSeedSchemaTest`
Expected: FAIL — overridden table currently still goes through `SHOW CREATE TABLE` (JDBC interaction / connection error), so `verifyNoInteractions` fails.

- [ ] **Step 3: Implement per-table selection**

In `TableDiscoveryUtils.java`, replace the body of the 4-arg
`discoverSchemaForCapturedTables(MySqlPartition, List<TableId>, MySqlSourceConfig, MySqlConnection)`
(currently lines 142-166) with:

```java
    public static Map<TableId, TableChange> discoverSchemaForCapturedTables(
            MySqlPartition partition,
            List<TableId> capturedTableIds,
            MySqlSourceConfig sourceConfig,
            MySqlConnection jdbc) {
        if (capturedTableIds.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Can't find any matched tables, please check your configured database-name: %s and table-name: %s",
                            sourceConfig.getDatabaseList(), sourceConfig.getTableList()));
        }

        final Map<String, String> seedSchemas = sourceConfig.getSeedSchemas();
        warnOnUnknownSeedKeys(seedSchemas, capturedTableIds);

        // fetch table schemas
        try (MySqlSchema mySqlSchema =
                new MySqlSchema(sourceConfig, jdbc.isTableIdCaseSensitive())) {
            Map<TableId, TableChange> tableSchemas = new HashMap<>();
            for (TableId tableId : capturedTableIds) {
                String seedDdl = seedSchemas.get(seedKey(tableId));
                TableChange tableSchema;
                if (seedDdl != null) {
                    tableSchema = mySqlSchema.parseTableSchema(partition, tableId, seedDdl);
                } else {
                    tableSchema = mySqlSchema.getTableSchema(partition, jdbc, tableId);
                }
                tableSchemas.put(tableId, tableSchema);
            }
            return tableSchemas;
        }
    }

    private static String seedKey(TableId tableId) {
        return tableId.catalog() + "." + tableId.table();
    }

    private static void warnOnUnknownSeedKeys(
            Map<String, String> seedSchemas, List<TableId> capturedTableIds) {
        if (seedSchemas.isEmpty()) {
            return;
        }
        Set<String> capturedKeys = new HashSet<>();
        for (TableId tableId : capturedTableIds) {
            capturedKeys.add(seedKey(tableId));
        }
        for (String key : seedSchemas.keySet()) {
            if (!capturedKeys.contains(key)) {
                LOG.warn(
                        "seedSchemas contains an entry for '{}' which is not a captured table; "
                                + "it will be ignored. Captured tables: {}",
                        key,
                        capturedKeys);
            }
        }
    }
```

Add imports at the top of the file if missing:

```java
import java.util.HashSet;
import java.util.Set;
```

If `TableDiscoveryUtils` has no logger, add near the top of the class body:

```java
    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(TableDiscoveryUtils.class);
```

> The `new MySqlSchema(sourceConfig, jdbc.isTableIdCaseSensitive())` call still invokes `isTableIdCaseSensitive()` once. If the test's `verifyNoInteractions(jdbc)` fails only because of that single stubbed call, apply the fallback noted in the test (use `verify(jdbc, never()).query(...)` instead). Do not add JDBC calls inside the override branch.

- [ ] **Step 4: Format, run test to verify it passes**

Run: `mvn -q -pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc spotless:apply && mvn -q -pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc -am test -Dtest=TableDiscoveryUtilsSeedSchemaTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc/src/main/java/org/apache/flink/cdc/connectors/mysql/source/utils/TableDiscoveryUtils.java \
        flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc/src/test/java/org/apache/flink/cdc/connectors/mysql/source/utils/TableDiscoveryUtilsSeedSchemaTest.java
git commit -m "feat(mysql-cdc): seed binlog schema from supplied DDL with per-table fallback

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: End-to-end integration test — cross-DDL cold start + negative control

**Files:**
- Test: `src/test/java/org/apache/flink/cdc/connectors/mysql/source/MySqlSeedSchemaCrossDdlITCase.java`

**Interfaces:**
- Consumes: `MySqlSourceBuilder.seedSchemas(...)` (Task 1) and the full seeding path (Tasks 2-3).
- Produces: nothing (verification only).

- [ ] **Step 1: Write the failing test**

Create `src/test/java/org/apache/flink/cdc/connectors/mysql/source/MySqlSeedSchemaCrossDdlITCase.java`. Mirror setup from `MySqlSourceITCase` / `MySqlSourceTestBase` (uses `MYSQL_CONTAINER` + a raw JDBC connection). The scenario:

```java
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
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves a cold binlog start can cross a past DDL when the old schema is supplied via {@code
 * seedSchemas}, and that it still crashes without it (negative control).
 */
class MySqlSeedSchemaCrossDdlITCase extends MySqlSourceTestBase {

    // Empty init script: we create/populate the table by hand so we control the DDL timeline.
    private final UniqueDatabase db =
            new UniqueDatabase(MYSQL_CONTAINER, "seed_schema_it", "mysqluser", "mysqlpw");

    private String setUpCrossDdlTimeline() throws Exception {
        db.createAndInitialize();
        try (Connection conn = db.getJdbcConnection();
                Statement st = conn.createStatement()) {
            st.execute(
                    "CREATE TABLE orders (id INT NOT NULL, amount INT, PRIMARY KEY (id))");
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
                .serverId("5400-5404")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.earliest());
    }

    @Test
    void coldStartWithSeedSchemaCrossesDdl() throws Exception {
        String oldDdl = setUpCrossDdlTimeline();

        MySqlSource<String> source =
                baseBuilder()
                        .seedSchemas(
                                Collections.singletonMap(
                                        db.getDatabaseName() + ".orders", oldDdl))
                        .build();

        List<String> rows = runAndCollect(source, 3);

        // All three rows decoded; the post-DDL row carries the new column.
        assertThat(rows).hasSize(3);
        assertThat(rows).anyMatch(r -> r.contains("\"id\":3") && r.contains("post"));
    }

    @Test
    void coldStartWithoutSeedSchemaStillCrashes() throws Exception {
        setUpCrossDdlTimeline();
        MySqlSource<String> source = baseBuilder().build(); // no seedSchemas

        assertThatThrownBy(() -> runAndCollect(source, 3))
                .hasStackTraceContaining("internal schema representation");
    }

    private List<String> runAndCollect(MySqlSource<String> source, int expected) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000L);
        DataStreamSource<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "seed-schema-it");
        List<String> out = new ArrayList<>();
        try (CloseableIterator<String> it = stream.executeAndCollect()) {
            while (it.hasNext() && out.size() < expected) {
                out.add(it.next());
            }
        }
        return out;
    }
}
```

> Adjustments to verify against `MySqlSourceITCase` before running: exact `UniqueDatabase` constructor arity, whether `serverId` range must be unique across the test class, and the precise substring in the failure (`"internal schema representation"` vs `"Data row is smaller than a column index"` — grep the Debezium message and match it). If `executeAndCollect` blocks when the crash happens, wrap the negative-control assertion to fail fast via the job's exception rather than iterator timeout.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc verify -Dtest=MySqlSeedSchemaCrossDdlITCase`
Expected: `coldStartWithSeedSchemaCrossesDdl` FAILS before Tasks 1-3 are wired end-to-end (or if run in isolation, confirms the crash path). After Tasks 1-3, the positive test should pass and the negative one confirm the crash.

- [ ] **Step 3: No new implementation**

This task adds no production code — it exercises Tasks 1-3 end-to-end. If the positive test fails, debug the seeding path (use `superpowers:systematic-debugging`); do not weaken the assertions.

- [ ] **Step 4: Run both tests to verify pass**

Run: `mvn -q -pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc verify -Dtest=MySqlSeedSchemaCrossDdlITCase`
Expected: PASS — positive test crosses the DDL and decodes all 3 rows; negative test still crashes.

- [ ] **Step 5: Commit**

```bash
git add flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc/src/test/java/org/apache/flink/cdc/connectors/mysql/source/MySqlSeedSchemaCrossDdlITCase.java
git commit -m "test(mysql-cdc): cross-DDL cold-start reprocessing with seed schema + negative control

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Full module build + regression sweep

**Files:** none (verification).

- [ ] **Step 1: Spotless + Checkstyle**

Run: `mvn -q -pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc spotless:apply checkstyle:check`
Expected: BUILD SUCCESS.

- [ ] **Step 2: Full module test**

Run: `mvn -pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc -am clean install`
Expected: BUILD SUCCESS with no new failures. Confirms default behavior (empty `seedSchemas`) is unchanged — every pre-existing test still passes.

- [ ] **Step 3: Commit (only if any formatting/nits were auto-applied)**

```bash
git add -A
git commit -m "chore(mysql-cdc): formatting for seed-schema override

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review Notes

- **Spec coverage:** Public API (Task 1) ✔; DDL→TableChange reuse (Task 2) ✔; per-table selection + fallback (Task 3) ✔; guardrail WARNs — startup-mode (Task 1) + unknown-key (Task 3) ✔; unit tests 1-2 (Tasks 1-3) ✔; integration + negative control (Task 4) ✔; default-unchanged (Task 5) ✔. CDH wiring is explicitly out of scope (spec §Downstream).
- **Type consistency:** `getSeedSchemas()`/`seedSchemas(Map<String,String>)` names and `Map<String,String>` type are identical across Tasks 1, 3, 4. `parseTableSchema(MySqlPartition, TableId, String)` identical in Tasks 2-3. `seedKey(TableId)` used only within Task 3.
- **Known verify-before-trust points** (flagged inline, not placeholders): `StartupMode` enum constant names; the exact Debezium failure substring for the negative control; `UniqueDatabase` constructor arity; Mockito `verifyNoInteractions` vs the single `isTableIdCaseSensitive()` call.
