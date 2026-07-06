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

import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Tests that seed-schema overrides bypass JDBC on cold start, and are NOT applied on the
 * scan-newly-added-tables path (which must always reflect the live DB schema).
 */
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
        MySqlPartition partition =
                new MySqlPartition(config.getMySqlConnectorConfig().getLogicalName());

        // discoverSeededSchemaForCapturedTables backs the cold-start entry point
        // (discoverSchemaForCapturedTables(partition, sourceConfig, jdbc)) exclusively.
        Map<TableId, TableChange> schemas =
                TableDiscoveryUtils.discoverSeededSchemaForCapturedTables(
                        partition, Collections.singletonList(orders), config, jdbc);

        assertThat(schemas).containsKey(orders);
        assertThat(schemas.get(orders).getTable().columns())
                .extracting(io.debezium.relational.Column::name)
                .containsExactly("id");
        // The whole point: no SHOW CREATE TABLE / DESC query was issued for the overridden table.
        // (MySqlSchema's constructor still calls jdbc.isTableIdCaseSensitive() once, so we can't
        // use verifyNoInteractions here; assert instead that no JDBC query was ever issued.)
        verify(jdbc, never()).query(anyString(), any());
    }

    @Test
    void newAddedTablesPathIgnoresSeedSchemas() throws Exception {
        // Regression guard for I1: a table added mid-job via scan-newly-added-tables must never
        // be seeded from stale DDL, even when its db.table key matches a seedSchemas entry.
        TableId orders = new TableId("mydb", null, "orders");
        MySqlSourceConfig config =
                configWithSeed(
                        Collections.singletonMap(
                                "mydb.orders",
                                "CREATE TABLE `orders` (`id` INT NOT NULL, PRIMARY KEY (`id`))"));

        MySqlConnection jdbc = mock(MySqlConnection.class);
        MySqlPartition partition =
                new MySqlPartition(config.getMySqlConnectorConfig().getLogicalName());

        // discoverSchemaForNewAddedTables() delegates to the pure (non-seed-aware)
        // discoverSchemaForCapturedTables(partition, List, sourceConfig, jdbc) overload once it
        // has computed the newly-added table list; exercise that overload directly here (as
        // MySqlSourceReader's "existing schemas non-empty" branch does) with a table list that
        // already contains the seeded table id, matching the shape of a newly-discovered table.
        assertThatThrownBy(
                        () ->
                                TableDiscoveryUtils.discoverSchemaForCapturedTables(
                                        partition, Collections.singletonList(orders), config, jdbc))
                .isInstanceOf(FlinkRuntimeException.class);

        // The seed DDL was ignored: MySqlSchema fell through to the real JDBC path (SHOW CREATE
        // TABLE, then DESC as fallback), which is why the mocked connection (returning no rows)
        // ultimately fails to resolve a schema.
        verify(jdbc).query(startsWith("SHOW CREATE TABLE"), any());
        verify(jdbc).query(startsWith("DESC "), any());
    }
}
