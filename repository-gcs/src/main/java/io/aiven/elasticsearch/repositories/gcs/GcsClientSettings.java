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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.settings.SecureString;

import io.aiven.elasticsearch.repositories.CommonSettings;

import com.google.auth.oauth2.GoogleCredentials;

import static io.aiven.elasticsearch.repositories.CommonSettings.ClientSettings.getConfigValue;
import static io.aiven.elasticsearch.repositories.CommonSettings.ClientSettings.readInputStream;
import static org.opensearch.common.settings.SecureSetting.secureFile;
import static org.opensearch.common.settings.SecureSetting.secureString;
import static org.opensearch.common.settings.Setting.intSetting;
import static org.opensearch.common.settings.Setting.simpleString;

/**
 * Settings for Google Cloud Storage client.
 * Some of the settings are using fallback to the old settings when the default client is used, or some of
 * the settings are not present for the named client. This is to keep backward compatibility with the old settings.
 */
public class GcsClientSettings implements CommonSettings.ClientSettings {

    static final String GCS_PREFIX = AIVEN_PREFIX + "gcs.client.";

    public static final Setting.AffixSetting<InputStream> PUBLIC_KEY_FILE =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "public_key_file",
                    key -> secureFile(key, null)
            );

    public static final Setting.AffixSetting<InputStream> PRIVATE_KEY_FILE =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "private_key_file",
                    // Fallback to old setting name for private key, when the default client is used.
                    key -> secureFile(key, null)
            );

    public static final Setting.AffixSetting<InputStream> CREDENTIALS_FILE_SETTING =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "credentials_file",
                    // Fallback to old setting name for credentials file, when the default client is used.
                    key -> secureFile(key, null)
            );

    public static final Setting.AffixSetting<String> PROXY_HOST =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "proxy.host",
                    key -> simpleString(key, Setting.Property.NodeScope)
            );

    public static final Setting.AffixSetting<Integer> PROXY_PORT =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "proxy.port",
                    key -> intSetting(key,
                            0,
                            Setting.Property.NodeScope)
            );

    public static final Setting.AffixSetting<SecureString> PROXY_USER_NAME =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "proxy.user_name",
                    key -> secureString(key, null)
            );

    public static final Setting.AffixSetting<SecureString> PROXY_USER_PASSWORD =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "proxy.user_password",
                    key -> secureString(key, null)
            );

    public static final Setting.AffixSetting<String> PROJECT_ID =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "project_id",
                    key -> simpleString(key, Setting.Property.NodeScope)
            );

    public static final Setting.AffixSetting<Integer> CONNECTION_TIMEOUT =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "connection_timeout",
                    key -> Setting.intSetting(key, -1, Setting.Property.NodeScope)
            );

    public static final Setting.AffixSetting<Integer> READ_TIMEOUT =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "read_timeout",
                    key -> Setting.intSetting(key, -1, Setting.Property.NodeScope)
            );

    /**
     * The number of retries to use when an GCS request fails.
     */
    public static final Setting.AffixSetting<Integer> MAX_RETRIES_SETTING =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "max_retries",
                    key -> Setting.intSetting(key, 0, Setting.Property.NodeScope)
            );

    private final byte[] publicKey;

    private final byte[] privateKey;

    private final byte[] gcsCredentials;

    private final String projectId;

    private final int connectionTimeout;

    private final int readTimeout;

    /**
     * The number of retries to use for the GCS client.
     */
    private final int maxRetries;

    private final String proxyUsername;

    private final char[] proxyUserPassword;

    private final String proxyHost;

    private final int proxyPort;

    private GcsClientSettings(final byte[] publicKey,
                              final byte[] privateKey,
                              final String projectId,
                              final byte[] gcsCredentials,
                              final int connectionTimeout,
                              final int readTimeout,
                              final int maxRetries,
                              final String proxyHost,
                              final int proxyPort,
                              final String proxyUsername,
                              final char[] proxyUserPassword) {
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
    }

    public static Map<String, GcsClientSettings> create(final Settings settings) throws IOException {
        if (settings.isEmpty()) {
            throw new IllegalArgumentException("Settings for GC storage hasn't been set");
        }
        final var clientSettings = new HashMap<String, GcsClientSettings>();
        final Set<String> clientNames = settings.getGroups(GCS_PREFIX, true).keySet();
        for (final var clientName : clientNames) {
            clientSettings.put(clientName, createSettings(clientName, settings));
        }
        return Map.copyOf(clientSettings);
    }

    private static boolean hasLegacyDefaultOrLegacyCredentials(final Settings settings) {
        return getConfigValue(settings, DEFAULT_CLIENT_NAME, CREDENTIALS_FILE_SETTING) != null
                || getConfigValue(settings, DEFAULT_CLIENT_NAME, PUBLIC_KEY_FILE) != null
                || getConfigValue(settings, DEFAULT_CLIENT_NAME, PRIVATE_KEY_FILE) != null;
    }

    private static GcsClientSettings createSettings(
            final String clientName, final Settings settings) throws IOException {
        if (PROXY_PORT.getConcreteSettingForNamespace(clientName).exists(settings)
                && PROXY_PORT.getConcreteSettingForNamespace(clientName).get(settings) < 0) {
            throw new IllegalArgumentException("Settings with name " + PROXY_PORT.getKey() + " must be greater than 0");
        }
        return new GcsClientSettings(
                readInputStream(getConfigValue(settings, clientName, PUBLIC_KEY_FILE)),
                readInputStream(getConfigValue(settings, clientName, PRIVATE_KEY_FILE)),
                getConfigValue(settings, clientName, PROJECT_ID),
                readInputStream(getConfigValue(settings, clientName, CREDENTIALS_FILE_SETTING)),
                getConfigValue(settings, clientName, CONNECTION_TIMEOUT),
                getConfigValue(settings, clientName, READ_TIMEOUT),
                getConfigValue(settings, clientName, MAX_RETRIES_SETTING),
                getConfigValue(settings, clientName, PROXY_HOST),
                getConfigValue(settings, clientName, PROXY_PORT),
                getConfigValue(settings, clientName, PROXY_USER_NAME).toString(),
                getConfigValue(settings, clientName, PROXY_USER_PASSWORD).getChars()
        );
    }

    private static GoogleCredentials loadCredentials(
            final Settings settings,
            final String clientName) throws IOException {
        try (final var in = getConfigValue(settings, clientName, CREDENTIALS_FILE_SETTING)) {
            return GoogleCredentials.fromStream(in);
        }
    }

    @Override
    public byte[] publicKey() {
        return publicKey;
    }

    @Override
    public byte[] privateKey() {
        return privateKey;
    }

    public String projectId() {
        return projectId;
    }

    public byte[] gcsCredentials() {
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

}
