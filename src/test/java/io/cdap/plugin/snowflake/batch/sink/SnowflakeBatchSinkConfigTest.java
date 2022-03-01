/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.snowflake.batch.sink;

import io.cdap.cdap.api.macro.Macros;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.snowflake.sink.batch.SnowflakeBatchSinkConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.FieldSetter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SnowflakeBatchSinkConfigTest {
    private static final String MOCK_STAGE = "mockStage";

    @Test
    public void testBatchSinkDatabaseMacro() throws Exception {
        SnowflakeBatchSinkConfig config = new SnowflakeBatchSinkConfig("test", "test",
                "${database}", "test", "test", "test",
                null, null, null, null, null, null, null,
        null);

        Set<String> macroFields = new HashSet<>();
        macroFields.add(SnowflakeBatchSinkConfig.NAME_DATABASE);
        Set<String> lookupProperties = new HashSet<>();
        lookupProperties.add("database");
        Map<String, String> properties = new HashMap<>();
        properties.put(SnowflakeBatchSinkConfig.NAME_DATABASE, "${database}");
        Macros macros = new Macros(lookupProperties, null);

        PluginProperties rawProperties = PluginProperties.builder()
                .addAll(properties)
                .build()
                .setMacros(macros);

        FieldSetter.setField(config, SnowflakeBatchSinkConfig.class.getDeclaredField("referenceName"), "batch_sink");
        FieldSetter.setField(config, PluginConfig.class.getDeclaredField("rawProperties"), rawProperties);
        FieldSetter.setField(config, PluginConfig.class.getDeclaredField("macroFields"), macroFields);

        MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
        config.validate(collector);

        Assert.assertEquals(0, collector.getValidationFailures().size());
    }
}
