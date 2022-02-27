package io.cdap.plugin.snowflake.batch.sink;

import io.cdap.cdap.api.macro.Macros;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.snowflake.common.BaseSnowflakeConfig;
import io.cdap.plugin.snowflake.sink.batch.SnowflakeSinkConfig;
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
        SnowflakeSinkConfig config = new SnowflakeSinkConfig("test", "test",
                "${database}", "test", "test", "test",
                null, null, null, null, null, null, null,
        null);

        Set<String> macroFields = new HashSet<>();
        macroFields.add(SnowflakeSinkConfig.PROPERTY_DATABASE);
        Set<String> lookupProperties = new HashSet<>();
        lookupProperties.add("database");
        Map<String, String> properties = new HashMap<>();
        properties.put(SnowflakeSinkConfig.PROPERTY_DATABASE, "${database}");
        Macros macros = new Macros(lookupProperties, null);

        PluginProperties rawProperties = PluginProperties.builder()
                .addAll(properties)
                .build()
                .setMacros(macros);

        FieldSetter.setField(config, SnowflakeSinkConfig.class.getDeclaredField("referenceName"), "batch_sink");
        FieldSetter.setField(config, PluginConfig.class.getDeclaredField("rawProperties"), rawProperties);
        FieldSetter.setField(config, PluginConfig.class.getDeclaredField("macroFields"), macroFields);

        MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
        config.validate(collector);

        Assert.assertEquals(0, collector.getValidationFailures().size());
    }
}
