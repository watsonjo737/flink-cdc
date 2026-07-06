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
