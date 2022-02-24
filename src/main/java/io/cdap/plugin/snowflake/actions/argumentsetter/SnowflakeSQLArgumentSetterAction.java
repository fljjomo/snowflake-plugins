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

package io.cdap.plugin.snowflake.actions.argumentsetter;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.snowflake.common.client.SnowflakeAccessor;
import net.snowflake.client.jdbc.SnowflakeBasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Runs an arbitrary SQL query on Snowflake.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("SnowflakeSQLArgumentSetter")
@Description("Sets runtime arguments from an arbitrary SQL query on Snowflake.")
public class SnowflakeSQLArgumentSetterAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeSQLArgumentSetterAction.class);

  private final SnowflakeSQLArgumentSetterConfig config;

  public SnowflakeSQLArgumentSetterAction(SnowflakeSQLArgumentSetterConfig config) {
    this.config = config;
  }

  @Override
  public void run(ActionContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    SnowflakeAccessor snowflakeAccessor = new SnowflakeAccessor(config);
    SnowflakeBasicDataSource dataSource = snowflakeAccessor.getDataSource();

    PreparedStatement populateStmt = null;
    Connection connection =  dataSource.getConnection();
    ResultSet resultSet = null;
    String query = config.getQuery();
    try {
      populateStmt = connection.prepareStatement(query);
      resultSet = populateStmt.executeQuery();

      Map<String, String> argumentsMap = new HashMap<>();

      int count = 0;
      if (resultSet.next()) {
        count++;

        int columnCount = resultSet.getMetaData().getColumnCount();
        LOG.trace("Number of columns: " + columnCount);
        if (columnCount < 1) {
          throw new RuntimeException("No columns in result");
        }
        for (int i = 1; i <= columnCount; i++) {
          String columnLabel = resultSet.getMetaData().getColumnLabel(i);
          if (columnLabel == null || columnLabel.equalsIgnoreCase("")) {
            throw new RuntimeException("No column name returned");
          }
          LOG.debug("Column Label: " + columnLabel);
          String value = resultSet.getString(i);
          if (value == null) {
            throw new RuntimeException(String.format("No value was returned for column %s", columnLabel));
          }
          argumentsMap.put(columnLabel, value);
        }
      }
      if (count == 0) {
        throw new RuntimeException(String.format("The query result total rows should be \"1\" but is \"%d\"", count));
      }
      if (resultSet.next()) {
        throw new RuntimeException(String.format("The query result total rows should be \"1\" but is larger than \"1\""));
      }

      for (Map.Entry<String, String> argument : argumentsMap.entrySet()) {
        context.getArguments().set(argument.getKey(), argument.getValue());
      }
    } catch (SQLException e) {
      throw new IOException(String.format("Statement '%s' failed due to '%s'", query, e.getMessage()), e);
    } finally {
      if (!resultSet.isClosed()) {
        resultSet.close();
      }
      if (!populateStmt.isClosed()) {
        populateStmt.close();
      }
      if (!connection.isClosed()) {
        connection.close();
      }
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    config.validate(stageConfigurer.getFailureCollector());
  }
}
