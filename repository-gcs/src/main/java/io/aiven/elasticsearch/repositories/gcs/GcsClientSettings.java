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

import io.aiven.elasticsearch.repositories.CommonSettings;

import com.google.auth.oauth2.GoogleCredentials;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import static io.aiven.elasticsearch.repositories.CommonSettings.ClientSettings.checkSettings;
import static io.aiven.elasticsearch.repositories.CommonSettings.ClientSettings.getConfigValue;
import static io.aiven.elasticsearch.repositories.CommonSettings.ClientSettings.readInputStream;

public class GcsClientSettings implements CommonSettings.ClientSettings {

    static final String GCS_PREFIX = AIVEN_PREFIX + "gcs.";

    public static final Setting.AffixSetting<InputStream> PUBLIC_KEY_FILE =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "public_key_file",
                    key -> SecureSetting.secureFile(key, null)
            );

    public static final Setting.AffixSetting<InputStream> PRIVATE_KEY_FILE =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "private_key_file",
                    key -> SecureSetting.secureFile(key, null)
            );

    public static final Setting.AffixSetting<InputStream> CREDENTIALS_FILE_SETTING =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "client.credentials_file",
                    key -> SecureSetting.secureFile(key, null)
            );

    public static final Setting.AffixSetting<String> PROXY_HOST =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "client.proxy.host",
                    key -> Setting.simpleString(key, Setting.Property.NodeScope)
            );

    public static final Setting.AffixSetting<Integer> PROXY_PORT =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "client.proxy.port",
                    key -> SecureSetting.intSetting(key, 0, 0, Setting.Property.NodeScope)
            );

    public static final Setting.AffixSetting<SecureString> PROXY_USER_NAME =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "client.proxy.user_name",
                    key -> SecureSetting.secureString(key, null)
            );

    public static final Setting.AffixSetting<SecureString> PROXY_USER_PASSWORD =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "client.proxy.user_password",
                    key -> SecureSetting.secureString(key, null)
            );

    public static final Setting.AffixSetting<String> PROJECT_ID =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "client.project_id",
                    key -> Setting.simpleString(key, Setting.Property.NodeScope)
            );

    public static final Setting.AffixSetting<Integer> CONNECTION_TIMEOUT =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "client.connection_timeout",
                    key -> Setting.intSetting(key, -1, -1, Setting.Property.NodeScope)
            );

    public static final Setting.AffixSetting<Integer> READ_TIMEOUT =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "client.read_timeout",
                    key -> Setting.intSetting(key, -1, -1, Setting.Property.NodeScope)
            );

    /** The number of retries to use when an GCS request fails. */
    public static final Setting.AffixSetting<Integer> MAX_RETRIES_SETTING =
            Setting.affixKeySetting(
                    GCS_PREFIX,
                    "client.max_retries",
                    key -> Setting.intSetting(key, 3, 0, Setting.Property.NodeScope)
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
        final Set<String> clientNames = settings.getGroups(GCS_PREFIX).keySet();
        final var clientSettings = new HashMap<String, GcsClientSettings>();
        for (final var clientName : clientNames) {
            clientSettings.put(clientName, createSettings(clientName, settings));
        }
        return Map.copyOf(clientSettings);
    }

    private static GcsClientSettings createSettings(
            final String clientName, final Settings settings) throws IOException {
        checkSettings(CREDENTIALS_FILE_SETTING, clientName, settings);
        checkSettings(PUBLIC_KEY_FILE, clientName, settings);
        checkSettings(PRIVATE_KEY_FILE, clientName, settings);
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
}
