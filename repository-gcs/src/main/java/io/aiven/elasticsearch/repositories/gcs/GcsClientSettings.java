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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.SecureString;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

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
                key -> secureFile(key, LegacyFallback.PUBLIC_KEY_FILE)
            );

    public static final Setting.AffixSetting<InputStream> PRIVATE_KEY_FILE =
            Setting.affixKeySetting(
                GCS_PREFIX,
                "private_key_file",
                // Fallback to old setting name for private key, when the default client is used.
                key -> secureFile(key, LegacyFallback.PRIVATE_KEY_FILE)
            );

    public static final Setting.AffixSetting<InputStream> CREDENTIALS_FILE_SETTING =
            Setting.affixKeySetting(
                GCS_PREFIX,
                "credentials_file",
                // Fallback to old setting name for credentials file, when the default client is used.
                key -> secureFile(key, LegacyFallback.CREDENTIALS_FILE_SETTING)
            );

    public static final Setting.AffixSetting<String> PROXY_HOST =
            Setting.affixKeySetting(
                GCS_PREFIX,
                "proxy.host",
                key -> simpleString(key,
                                    LegacyFallback.PROXY_HOST,
                                    Setting.Property.NodeScope)
            );

    public static final Setting.AffixSetting<Integer> PROXY_PORT =
            Setting.affixKeySetting(
                GCS_PREFIX,
                "proxy.port",
                key -> intSetting(key,
                                  LegacyFallback.PROXY_PORT,
                                  0,
                                  Setting.Property.NodeScope)
            );

    public static final Setting.AffixSetting<SecureString> PROXY_USER_NAME =
            Setting.affixKeySetting(
                GCS_PREFIX,
                "proxy.user_name",
                key -> secureString(key, LegacyFallback.PROXY_USER_NAME)
            );

    public static final Setting.AffixSetting<SecureString> PROXY_USER_PASSWORD =
            Setting.affixKeySetting(
                GCS_PREFIX,
                "proxy.user_password",
                key -> secureString(key, LegacyFallback.PROXY_USER_PASSWORD)
            );

    public static final Setting.AffixSetting<String> PROJECT_ID =
            Setting.affixKeySetting(
                GCS_PREFIX,
                "project_id",
                key -> simpleString(key, LegacyFallback.PROJECT_ID, Setting.Property.NodeScope)
            );

    public static final Setting.AffixSetting<Integer> CONNECTION_TIMEOUT =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "connection_timeout",
                    key -> Setting.intSetting(key, LegacyFallback.CONNECTION_TIMEOUT, -1, Setting.Property.NodeScope)
            );

    public static final Setting.AffixSetting<Integer> READ_TIMEOUT =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "read_timeout",
                    key -> Setting.intSetting(key, LegacyFallback.READ_TIMEOUT, -1, Setting.Property.NodeScope)
            );

    /** The number of retries to use when an GCS request fails. */
    public static final Setting.AffixSetting<Integer> MAX_RETRIES_SETTING =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "max_retries",
                    key -> Setting.intSetting(key, LegacyFallback.MAX_RETRIES_SETTING, 0, Setting.Property.NodeScope)
            );

    private final byte[] publicKey;

    private final byte[] privateKey;

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

    private GcsClientSettings(final byte[] publicKey,
                              final byte[] privateKey,
                              final String projectId,
                              final GoogleCredentials gcsCredentials,
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
        final var filteredSettings = settings.filter(key -> !LegacyFallback.KEY_MAP.containsKey(key));
        final Set<String> clientNames = filteredSettings.getGroups(GCS_PREFIX, true).keySet();
        for (final var clientName : clientNames) {
            clientSettings.put(clientName, createSettings(clientName, settings));
        }

        if (!clientSettings.containsKey(DEFAULT_CLIENT_NAME) && hasLegacyDefaultOrLegacyCredentials(settings)) {
            // this won't find any settings under the default client,
            // but it will pull all the fallback static settings
            clientSettings.put(DEFAULT_CLIENT_NAME, createSettings(DEFAULT_CLIENT_NAME, settings));
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
        CommonSettings.ClientSettings.checkSettings(CREDENTIALS_FILE_SETTING, clientName, settings);
        CommonSettings.ClientSettings.checkSettings(PUBLIC_KEY_FILE, clientName, settings);
        CommonSettings.ClientSettings.checkSettings(PRIVATE_KEY_FILE, clientName, settings);
        if (PROXY_PORT.getConcreteSettingForNamespace(clientName).exists(settings)
                && PROXY_PORT.getConcreteSettingForNamespace(clientName).get(settings) < 0) {
            throw new IllegalArgumentException("Settings with name " + PROXY_PORT.getKey() + " must be greater than 0");
        }
        return new GcsClientSettings(
                readInputStream(getConfigValue(settings, clientName, PUBLIC_KEY_FILE)),
                readInputStream(getConfigValue(settings, clientName, PRIVATE_KEY_FILE)),
                getConfigValue(settings, clientName, PROJECT_ID),
                loadCredentials(settings, clientName),
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

    public static class LegacyFallback {
        static final Setting<InputStream> PUBLIC_KEY_FILE =
            SecureSetting.secureFile("aiven.gcs.public_key_file", null);
        static final Setting<InputStream> PRIVATE_KEY_FILE =
            SecureSetting.secureFile("aiven.gcs.private_key_file", null);
        static final Setting<InputStream> CREDENTIALS_FILE_SETTING =
            SecureSetting.secureFile("aiven.gcs.client.credentials_file", null);
        static final Setting<String> PROXY_HOST =
            Setting.simpleString("aiven.gcs.client.proxy.host", Setting.Property.NodeScope);
        static final Setting<Integer> PROXY_PORT =
            SecureSetting.intSetting("aiven.gcs.client.proxy.port", 0, 0, Setting.Property.NodeScope);
        static final Setting<SecureString> PROXY_USER_NAME =
            SecureSetting.secureString("aiven.gcs.client.proxy.user_name", null);
        static final Setting<SecureString> PROXY_USER_PASSWORD =
            SecureSetting.secureString("aiven.gcs.client.proxy.user_password", null);
        static final Setting<String> PROJECT_ID =
            Setting.simpleString("aiven.gcs.client.project_id", Setting.Property.NodeScope);
        static final Setting<Integer> CONNECTION_TIMEOUT =
            Setting.intSetting("aiven.gcs.client.connection_timeout", -1, -1, Setting.Property.NodeScope);
        static final Setting<Integer> READ_TIMEOUT =
            Setting.intSetting("aiven.gcs.client.read_timeout", -1, -1, Setting.Property.NodeScope);
        static final Setting<Integer> MAX_RETRIES_SETTING =
            Setting.intSetting("aiven.gcs.client.max_retries", 3, 0, Setting.Property.NodeScope);

        public static final Map<String, Setting<?>> KEY_MAP;

        static {
            KEY_MAP = Stream.of(PUBLIC_KEY_FILE, PRIVATE_KEY_FILE, CREDENTIALS_FILE_SETTING, PROXY_HOST, PROXY_PORT,
                                PROXY_USER_NAME, PROXY_USER_PASSWORD, PROJECT_ID, CONNECTION_TIMEOUT, READ_TIMEOUT,
                                MAX_RETRIES_SETTING)
                            .collect(Collectors.toMap(Setting::getKey, s -> s));
        }
    }
}
