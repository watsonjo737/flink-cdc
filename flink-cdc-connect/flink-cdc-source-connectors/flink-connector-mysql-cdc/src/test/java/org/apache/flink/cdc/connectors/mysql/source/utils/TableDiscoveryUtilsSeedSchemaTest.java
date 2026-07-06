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

import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

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
        // (MySqlSchema's constructor still calls jdbc.isTableIdCaseSensitive() once, so we can't
        // use verifyNoInteractions here; assert instead that no JDBC query was ever issued.)
        verify(jdbc, never()).query(anyString(), any());
    }
}
