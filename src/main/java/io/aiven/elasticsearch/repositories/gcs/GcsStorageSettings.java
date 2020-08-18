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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.UserCredentials;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcsStorageSettings {

    private static final Logger LOGGER = LoggerFactory.getLogger(GcsStorageSettings.class);

    public static final Setting<InputStream> CREDENTIALS_FILE_SETTING =
            SecureSetting.secureFile("aiven.gcs.client.credentials_file", null);
    public static final Setting<String> PROJECT_ID =
            Setting.simpleString("aiven.gcs.client.project_id", Setting.Property.NodeScope);
    public static final Setting<Integer> CONNECTION_TIMEOUT =
            Setting.intSetting("aiven.gcs.client.connection_timeout", -1, -1, Setting.Property.NodeScope);
    public static final Setting<Integer> READ_TIMEOUT =
            Setting.intSetting("aiven.gcs.client.read_timeout", -1, -1, Setting.Property.NodeScope);

    private final String projectId;

    private final GoogleCredentials gcsCredentials;

    private final int connectionTimeout;

    private final int readTimeout;

    private GcsStorageSettings(final String projectId,
                               final GoogleCredentials gcsCredentials,
                               final int connectionTimeout,
                               final int readTimeout) {
        this.projectId = projectId;
        this.gcsCredentials = gcsCredentials;
        this.connectionTimeout = connectionTimeout;
        this.readTimeout = readTimeout;
    }

    public static GcsStorageSettings load(final Settings settings) throws IOException {
        if (settings.isEmpty()) {
            throw new IllegalArgumentException("Settings for GC storage hasn't been set");
        }
        return new GcsStorageSettings(
                PROJECT_ID.get(settings),
                loadCredentials(settings),
                CONNECTION_TIMEOUT.get(settings),
                READ_TIMEOUT.get(settings));
    }

    private static GoogleCredentials loadCredentials(final Settings settings) throws IOException {
        try (final var in = getStreamFor(CREDENTIALS_FILE_SETTING, settings)) {
            return UserCredentials.fromStream(in);
        }
    }

    private static InputStream getStreamFor(final Setting<InputStream> setting, final Settings settings) {
        if (setting.exists(settings)) {
            LOGGER.info("Load settings with name: {}", setting.getKey());
            return setting.get(settings);
        }
        throw new IllegalArgumentException("Settings with name " + setting.getKey() + " hasn't been set");
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

}
