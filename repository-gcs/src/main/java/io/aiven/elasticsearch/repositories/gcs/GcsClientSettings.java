/*
 * Copyright 2020 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.elasticsearch.repositories.gcs;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.settings.SecureString;

import io.aiven.elasticsearch.repositories.CommonSettings;

import com.google.auth.oauth2.GoogleCredentials;

import static io.aiven.elasticsearch.repositories.CommonSettings.ClientSettings.withPrefix;

public final class GcsClientSettings implements CommonSettings.ClientSettings {

    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(GcsClientSettings.class);

    public static final Setting<InputStream> PUBLIC_KEY_FILE =
            SecureSetting.secureFile(withPrefix("gcs.public_key_file"), null);

    public static final Setting<InputStream> PRIVATE_KEY_FILE =
            SecureSetting.secureFile(withPrefix("gcs.private_key_file"), null);

    public static final Setting<InputStream> CREDENTIALS_FILE_SETTING =
            SecureSetting.secureFile(withPrefix("gcs.client.credentials_file"), null);

    public static final Setting<String> PROXY_HOST =
            Setting.simpleString(withPrefix("gcs.client.proxy.host"), Setting.Property.NodeScope);

    public static final Setting<Integer> PROXY_PORT =
            SecureSetting.intSetting(withPrefix("gcs.client.proxy.port"), 0, 0,
                    Setting.Property.NodeScope);

    public static final Setting<SecureString> PROXY_USER_NAME =
            SecureSetting.secureString(withPrefix("gcs.client.proxy.user_name"), null);

    public static final Setting<SecureString> PROXY_USER_PASSWORD =
            SecureSetting.secureString(withPrefix("gcs.client.proxy.user_password"), null);

    public static final Setting<String> PROJECT_ID =
            Setting.simpleString(withPrefix("gcs.client.project_id"), Setting.Property.NodeScope);

    public static final Setting<Integer> CONNECTION_TIMEOUT =
            Setting.intSetting(withPrefix("gcs.client.connection_timeout"), -1, -1,
                    Setting.Property.NodeScope);

    public static final Setting<Integer> READ_TIMEOUT =
            Setting.intSetting(withPrefix("gcs.client.read_timeout"), -1, -1,
                    Setting.Property.NodeScope);

    /** The number of retries to use when an GCS request fails. */
    public static final Setting<Integer> MAX_RETRIES_SETTING =
            Setting.intSetting(withPrefix("gcs.client.max_retries"), 3, 0, 
                    Setting.Property.NodeScope);

    /** The client name to use for this repository instance. */
    public static final Setting<String> CLIENT_NAME =
            Setting.simpleString("client", "default", Setting.Property.NodeScope, Setting.Property.Dynamic);

    private final InputStream publicKey;

    private final InputStream privateKey;

    private final String projectId;

    private final GoogleCredentials gcsCredentials;

    private final int connectionTimeout;

    private final int readTimeout;

    /** The number of retries to use for the GCS client. */
    private final int maxRetries;

    private final String proxyUsername;

    private final char[] proxyUserPassword;

    private final String proxyHost;

    private final int proxyPort;

    private final String clientName;

    private GcsClientSettings(final InputStream publicKey,
                              final InputStream privateKey,
                              final String projectId,
                              final GoogleCredentials gcsCredentials,
                              final int connectionTimeout,
                              final int readTimeout,
                              final int maxRetries,
                              final String proxyHost,
                              final int proxyPort,
                              final String proxyUsername,
                              final char[] proxyUserPassword,
                              final String clientName) {
        this.publicKey = publicKey;
        this.privateKey = privateKey;
        this.projectId = projectId;
        this.gcsCredentials = gcsCredentials;
        this.connectionTimeout = connectionTimeout;
        this.readTimeout = readTimeout;
        this.maxRetries = maxRetries;
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.proxyUsername = proxyUsername;
        this.proxyUserPassword = proxyUserPassword;
        this.clientName = clientName;
    }

    /**
     * Creates GcsClientSettings from the given settings.
     * 
     * @param settings the settings to create GcsClientSettings from
     * @return GcsClientSettings instance
     * @throws IOException if an error occurs while reading the settings
     */
    public static GcsClientSettings create(final Settings settings) throws IOException {
        final var clientName = CLIENT_NAME.get(settings);
        
        // Use client-specific keystore keys if available, fall back to default
        final var credentialsFile = getClientSpecificCredentialsFile(settings, clientName);
        final var publicKeyFile = getClientSpecificPublicKeyFile(settings, clientName);
        final var privateKeyFile = getClientSpecificPrivateKeyFile(settings, clientName);
        
        // Check required settings
        if (credentialsFile == null) {
            throw new IllegalArgumentException("Missing required setting: " + CREDENTIALS_FILE_SETTING.getKey());
        }
        
        final var projectId = PROJECT_ID.get(settings);
        final var connectionTimeout = CONNECTION_TIMEOUT.get(settings);
        final var readTimeout = READ_TIMEOUT.get(settings);
        final var maxRetries = MAX_RETRIES_SETTING.get(settings);
        final var proxyHost = PROXY_HOST.get(settings);
        final var proxyPort = PROXY_PORT.get(settings);
        final var proxyUserName = PROXY_USER_NAME.get(settings);
        final var proxyUserPassword = PROXY_USER_PASSWORD.get(settings);
        
        return new GcsClientSettings(
                publicKeyFile,
                privateKeyFile,
                projectId,
                loadCredentialsFromStream(credentialsFile),
                connectionTimeout,
                readTimeout,
                maxRetries,
                proxyHost,
                proxyPort,
                proxyUserName.toString(),
                proxyUserPassword.getChars(),
                clientName
        );
    }
    
    /**
     * Creates GcsClientSettings from repository-specific settings, overriding plugin-level settings.
     * This method is used when we need to create settings with repository-specific client parameters.
     * 
     * @param pluginSettings the plugin-level settings (for keystore access)
     * @param repositorySettings the repository-specific settings (contains client parameter)
     * @return GcsClientSettings instance
     * @throws IOException if an error occurs while reading the settings
     */
    public static GcsClientSettings createFromRepositorySettings(final Settings pluginSettings, 
                                                               final Settings repositorySettings) throws IOException {
        final var clientName = CLIENT_NAME.get(repositorySettings);
        
        // Use client-specific keystore keys if available, fall back to default
        final var credentialsFile = getClientSpecificCredentialsFile(pluginSettings, clientName);
        final var publicKeyFile = getClientSpecificPublicKeyFile(pluginSettings, clientName);
        final var privateKeyFile = getClientSpecificPrivateKeyFile(pluginSettings, clientName);
        
        // Check required settings
        if (credentialsFile == null) {
            throw new IllegalArgumentException("Missing required setting: " + CREDENTIALS_FILE_SETTING.getKey());
        }
        
        final var projectId = PROJECT_ID.get(repositorySettings);
        final var connectionTimeout = CONNECTION_TIMEOUT.get(repositorySettings);
        final var readTimeout = READ_TIMEOUT.get(repositorySettings);
        final var maxRetries = MAX_RETRIES_SETTING.get(repositorySettings);
        final var proxyHost = PROXY_HOST.get(repositorySettings);
        final var proxyPort = PROXY_PORT.get(repositorySettings);
        final var proxyUserName = PROXY_USER_NAME.get(repositorySettings);
        final var proxyUserPassword = PROXY_USER_PASSWORD.get(repositorySettings);
        
        return new GcsClientSettings(
                publicKeyFile,
                privateKeyFile,
                projectId,
                loadCredentialsFromStream(credentialsFile),
                connectionTimeout,
                readTimeout,
                maxRetries,
                proxyHost,
                proxyPort,
                proxyUserName.toString(),
                proxyUserPassword.getChars(),
                clientName
        );
    }
    
    /**
     * Gets client-specific credentials file, falling back to default if not available.
     */
    private static InputStream getClientSpecificCredentialsFile(final Settings settings, final String clientName) {
        LOGGER.debug("Attempting to get client-specific credentials for client: '{}'", clientName);
        
        if ("default".equals(clientName)) {
            LOGGER.debug("Client is 'default', using default keystore key");
            return CREDENTIALS_FILE_SETTING.get(settings);
        }
        
        // Try client-specific keystore key first
        try {
            LOGGER.debug("Trying client-specific keystore key: aiven.gcs.client.{}.credentials_file", clientName);
            
            // Use a pattern-based approach to check if the client-specific key exists
            // This avoids the need to create dynamic settings
            final var clientSpecificKey = "aiven.gcs.client." + clientName + ".credentials_file";
            
            // Try to get the setting using the existing SecureSetting mechanism
            // If the key exists in the keystore, this will work
            // If not, it will throw an exception and we'll fall back to default
            final var clientSpecificSetting = SecureSetting.secureFile(clientSpecificKey, null);
            final var result = clientSpecificSetting.get(settings);
            
            LOGGER.debug("Successfully loaded client-specific credentials for client: '{}'", clientName);
            return result;
            
        } catch (final Exception e) {
            // If any error occurs (including missing keystore key), fall back to default
            LOGGER.debug("Client-specific credentials not found for client '{}', falling back to default. Error: {}", 
                        clientName, e.getMessage());
            return CREDENTIALS_FILE_SETTING.get(settings);
        }
    }
    
    /**
     * Gets client-specific public key file, falling back to default if not available.
     * Note: Public keys are always stored in the default keystore location.
     */
    private static InputStream getClientSpecificPublicKeyFile(final Settings settings, final String clientName) {
        // Public keys are always stored in the default keystore location
        return PUBLIC_KEY_FILE.get(settings);
    }
    
    /**
     * Gets client-specific private key file, falling back to default if not available.
     * Note: Private keys are always stored in the default keystore location.
     */
    private static InputStream getClientSpecificPrivateKeyFile(final Settings settings, final String clientName) {
        // Private keys are always stored in the default keystore location
        return PRIVATE_KEY_FILE.get(settings);
    }

    private static GoogleCredentials loadCredentialsFromStream(final InputStream inputStream) throws IOException {
        try (final var in = inputStream) {
            return GoogleCredentials.fromStream(in);
        }
    }

    public InputStream publicKey() {
        return publicKey;
    }

    public InputStream privateKey() {
        return privateKey;
    }

    public String projectId() {
        return projectId;
    }

    public GoogleCredentials gcsCredentials() {
        return gcsCredentials;
    }

    public int connectionTimeout() {
        return connectionTimeout > 0
                ? Math.toIntExact(TimeUnit.MILLISECONDS.toMillis(connectionTimeout))
                : connectionTimeout;
    }

    public int readTimeout() {
        return readTimeout > 0
                ? Math.toIntExact(TimeUnit.MILLISECONDS.toMillis(readTimeout))
                : readTimeout;
    }

    public String getProxyUsername() {
        return proxyUsername;
    }

    public char[] getProxyUserPassword() {
        return proxyUserPassword;
    }

    public String getProxyHost() {
        return proxyHost;
    }

    public int getProxyPort() {
        return proxyPort;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public String getClientName() {
        return clientName;
    }
}
