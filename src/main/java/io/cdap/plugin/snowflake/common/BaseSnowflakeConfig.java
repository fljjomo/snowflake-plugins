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

package io.cdap.plugin.snowflake.common;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.plugin.snowflake.common.client.SnowflakeAccessor;
import io.cdap.plugin.snowflake.common.exception.ConnectionTimeoutException;
import javax.annotation.Nullable;

// TODO: add get schema button
/**
 * Common configurations for Snowflake source and sink
 */
public class BaseSnowflakeConfig extends PluginConfig {
  public static final String NAME_ACCOUNT_NAME = "accountName";
  public static final String NAME_DATABASE = "database";
  public static final String NAME_SCHEMA_NAME = "schemaName";
  public static final String NAME_WAREHOUSE = "warehouse";
  public static final String PROPERTY_ROLE = "role";
  public static final String NAME_USERNAME = "username";
  public static final String PROPERTY_PASSWORD = "password";
  public static final String PROPERTY_KEY_PAIR_ENABLED = "keyPairEnabled";
  public static final String NAME_PRIVATE_KEY = "privateKey";
  public static final String NAME_PASSPHRASE = "passphrase";
  public static final String PROPERTY_OAUTH_2_ENABLED = "oauth2Enabled";
  public static final String PROPERTY_CLIENT_ID = "clientId";
  public static final String PROPERTY_CLIENT_SECRET = "clientSecret";
  public static final String PROPERTY_REFRESH_TOKEN = "refreshToken";
  public static final String PROPERTY_CONNECTION_ARGUMENTS = "connectionArguments";

  @Name(NAME_ACCOUNT_NAME)
  @Macro
  @Description("Full name of Snowflake account.")
  private String accountName;

  @Name(NAME_DATABASE)
  @Description("Database name to connect to.")
  @Macro
  private String database;

  @Name(NAME_SCHEMA_NAME)
  @Description("Schema name to connect to.")
  @Macro
  private String schemaName;

  @Name(NAME_WAREHOUSE)
  @Macro
  @Nullable
  @Description("Warehouse to connect to. If not specified default warehouse is used.")
  private String warehouse;

  @Name(PROPERTY_ROLE)
  @Macro
  @Nullable
  @Description("Role to use. If not specified default role is used.")
  private String role;

  @Name(NAME_USERNAME)
  @Macro
  @Nullable
  @Description("User identity for connecting to the specified database.")
  private String username;

  @Name(PROPERTY_PASSWORD)
  @Macro
  @Nullable
  @Description("Password to use to connect to the specified database.")
  private String password;

  @Name(PROPERTY_KEY_PAIR_ENABLED)
  @Description("If true, plugin will perform Key Pair authentication.")
  @Nullable
  private Boolean keyPairEnabled;

  @Name(NAME_PRIVATE_KEY)
  @Macro
  @Nullable
  @Description("Contents of the private key file.")
  private String privateKey;

  @Name(NAME_PASSPHRASE)
  @Macro
  @Nullable
  @Description("Passphrase for the private key file.")
  private String passphrase;

  @Name(PROPERTY_OAUTH_2_ENABLED)
  @Description("If true, plugin will perform OAuth2 authentication.")
  @Nullable
  private Boolean oauth2Enabled;

  @Name(PROPERTY_CLIENT_ID)
  @Description("Client identifier obtained during the Application registration process.")
  @Macro
  @Nullable
  private String clientId;

  @Name(PROPERTY_CLIENT_SECRET)
  @Description("Client secret obtained during the Application registration process.")
  @Macro
  @Nullable
  private String clientSecret;

  @Name(PROPERTY_REFRESH_TOKEN)
  @Description("Token used to receive accessToken, which is end product of OAuth2.")
  @Macro
  @Nullable
  private String refreshToken;

  @Name(PROPERTY_CONNECTION_ARGUMENTS)
  @Description("A list of arbitrary string tag/value pairs as connection arguments.")
  @Macro
  @Nullable
  private String connectionArguments;

  public BaseSnowflakeConfig(String accountName,
                             String database,
                             String schemaName,
                             String username,
                             String password,
                             @Nullable Boolean keyPairEnabled,
                             @Nullable String privateKey,
                             @Nullable String passphrase,
                             @Nullable Boolean oauth2Enabled,
                             @Nullable String clientId,
                             @Nullable String clientSecret,
                             @Nullable String refreshToken,
                             @Nullable String connectionArguments) {
    this.accountName = accountName;
    this.database = database;
    this.schemaName = schemaName;
    this.username = username;
    this.password = password;
    this.keyPairEnabled = keyPairEnabled;
    this.privateKey = privateKey;
    this.passphrase = passphrase;
    this.oauth2Enabled = oauth2Enabled;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.refreshToken = refreshToken;
    this.connectionArguments = connectionArguments;
  }

  public String getAccountName() {
    return accountName;
  }

  public String getDatabase() {
    return database;
  }

  public String getSchemaName() {
    return schemaName;
  }

  @Nullable
  public String getWarehouse() {
    return warehouse;
  }

  @Nullable
  public String getRole() {
    return role;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public Boolean getKeyPairEnabled() {
    return keyPairEnabled != null && keyPairEnabled;
  }

  @Nullable
  public String getPrivateKey() {
    return privateKey;
  }

  @Nullable
  public String getPassphrase() {
    return passphrase;
  }

  public Boolean getOauth2Enabled() {
    return oauth2Enabled != null && oauth2Enabled;
  }

  @Nullable
  public String getClientId() {
    return clientId;
  }

  @Nullable
  public String getClientSecret() {
    return clientSecret;
  }

  @Nullable
  public String getRefreshToken() {
    return refreshToken;
  }

  @Nullable
  public String getConnectionArguments() {
    return connectionArguments;
  }

  public void validate(FailureCollector collector) {
    if (getOauth2Enabled()) {
      if (!containsMacro(PROPERTY_CLIENT_ID)
        && Strings.isNullOrEmpty(getClientId())) {
        collector.addFailure("Client ID is not set.", null)
          .withConfigProperty(PROPERTY_CLIENT_ID);
      }
      if (!containsMacro(PROPERTY_CLIENT_SECRET)
        && Strings.isNullOrEmpty(getClientSecret())) {
        collector.addFailure("Client Secret is not set.", null)
          .withConfigProperty(PROPERTY_CLIENT_SECRET);
      }
      if (!containsMacro(PROPERTY_REFRESH_TOKEN)
        && Strings.isNullOrEmpty(getRefreshToken())) {
        collector.addFailure("Refresh Token is not set.", null)
          .withConfigProperty(PROPERTY_REFRESH_TOKEN);
      }
    } else if (getKeyPairEnabled()) {
      if (!containsMacro(NAME_USERNAME)
        && Strings.isNullOrEmpty(getUsername())) {
        collector.addFailure("Username is not set.", null)
          .withConfigProperty(NAME_USERNAME);
      }
      if (!containsMacro(NAME_PRIVATE_KEY)
        && Strings.isNullOrEmpty(getPrivateKey())) {
        collector.addFailure("Private Key is not set.", null)
          .withConfigProperty(NAME_PRIVATE_KEY);
      }
    } else {
      if (!containsMacro(NAME_USERNAME)
        && Strings.isNullOrEmpty(getUsername())) {
        collector.addFailure("Username is not set.", null)
          .withConfigProperty(NAME_USERNAME);
      }
      if (!containsMacro(PROPERTY_PASSWORD)
        && Strings.isNullOrEmpty(getPassword())) {
        collector.addFailure("Password is not set.", null)
          .withConfigProperty(PROPERTY_PASSWORD);
      }
    }
    validateConnection(collector);
  }

  /**
   * Returns true if connection properties don't contain any macros.
   */
  public boolean canConnect() {
    return (!containsMacro(NAME_DATABASE) && !containsMacro(NAME_SCHEMA_NAME)
      && !containsMacro(NAME_ACCOUNT_NAME) && !containsMacro(NAME_USERNAME)
      && !containsMacro(PROPERTY_PASSWORD) && !containsMacro(NAME_WAREHOUSE)
      && !containsMacro(PROPERTY_ROLE) && !containsMacro(PROPERTY_CLIENT_ID)
      && !containsMacro(PROPERTY_CLIENT_SECRET) && !containsMacro(PROPERTY_REFRESH_TOKEN)
      && !containsMacro(NAME_PRIVATE_KEY));
  }

  protected void validateConnection(FailureCollector collector) {
    if (!canConnect()) {
      return;
    }

    try {
      SnowflakeAccessor snowflakeAccessor = new SnowflakeAccessor(this);
      snowflakeAccessor.checkConnection();
    } catch (ConnectionTimeoutException e) {
      String reason = (e.getCause() == null) ? e.getMessage() : e.getCause().getMessage();

      ValidationFailure failure = collector.addFailure(
        String.format("There was an issue communicating with Snowflake API: '%s'.", reason), null)
        .withConfigProperty(NAME_ACCOUNT_NAME)
        .withConfigProperty(PROPERTY_ROLE)
        .withConfigProperty(NAME_USERNAME);

      // TODO: for oauth2
      if (keyPairEnabled) {
        failure.withConfigProperty(NAME_PRIVATE_KEY);
      } else {
        failure.withConfigProperty(PROPERTY_PASSWORD);
      }
    }
  }
}
