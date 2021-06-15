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

import io.aiven.elasticsearch.repositories.CommonSettings;

import com.google.auth.oauth2.GoogleCredentials;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import static io.aiven.elasticsearch.repositories.CommonSettings.KeystoreSettings.checkSettings;
import static io.aiven.elasticsearch.repositories.CommonSettings.KeystoreSettings.withPrefix;

public class GcsStorageSettings implements CommonSettings.KeystoreSettings {

    public static final Setting<InputStream> PUBLIC_KEY_FILE =
            SecureSetting.secureFile(withPrefix("gcs.public_key_file"), null);

    public static final Setting<InputStream> PRIVATE_KEY_FILE =
            SecureSetting.secureFile(withPrefix("gcs.private_key_file"), null);

    public static final Setting<InputStream> CREDENTIALS_FILE_SETTING =
            SecureSetting.secureFile(withPrefix("gcs.client.credentials_file"), null);

    public static final Setting<String> PROXY_HOST =
            Setting.simpleString(withPrefix("gcs.client.proxy.host"), Setting.Property.NodeScope);

    public static final Setting<Integer> PROXY_PORT =
            SecureSetting.intSetting(withPrefix("gcs.client.proxy.port"), DEFAULT_SOCKS5_PORT, 0,
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

    private final InputStream publicKey;

    private final InputStream privateKey;

    private final String projectId;

    private final GoogleCredentials gcsCredentials;

    private final int connectionTimeout;

    private final int readTimeout;

    private final String proxyUsername;

    private final char[] proxyUserPassword;

    private final String proxyHost;

    private final int proxyPort;

    private GcsStorageSettings(final InputStream publicKey,
                               final InputStream privateKey,
                               final String projectId,
                               final GoogleCredentials gcsCredentials,
                               final int connectionTimeout,
                               final int readTimeout,
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
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.proxyUsername = proxyUsername;
        this.proxyUserPassword = proxyUserPassword;
    }

    public static GcsStorageSettings create(final Settings settings) throws IOException {
        if (settings.isEmpty()) {
            throw new IllegalArgumentException("Settings for GC storage hasn't been set");
        }
        checkSettings(CREDENTIALS_FILE_SETTING, settings);
        checkSettings(PUBLIC_KEY_FILE, settings);
        checkSettings(PRIVATE_KEY_FILE, settings);
        if (PROXY_PORT.exists(settings) && PROXY_PORT.get(settings) < 0) {
            throw new IllegalArgumentException("Settings with name " + PROXY_PORT.getKey() + " must be greater than 0");
        }
        return new GcsStorageSettings(
                PUBLIC_KEY_FILE.get(settings),
                PRIVATE_KEY_FILE.get(settings),
                PROJECT_ID.get(settings),
                loadCredentials(settings),
                CONNECTION_TIMEOUT.get(settings),
                READ_TIMEOUT.get(settings),
                PROXY_HOST.get(settings),
                PROXY_PORT.get(settings),
                PROXY_USER_NAME.get(settings).toString(),
                PROXY_USER_PASSWORD.get(settings).getChars());
    }

    private static GoogleCredentials loadCredentials(final Settings settings) throws IOException {
        try (final var in = CREDENTIALS_FILE_SETTING.get(settings)) {
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

}
