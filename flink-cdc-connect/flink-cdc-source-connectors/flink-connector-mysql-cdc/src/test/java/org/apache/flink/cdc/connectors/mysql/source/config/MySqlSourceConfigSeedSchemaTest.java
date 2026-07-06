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
