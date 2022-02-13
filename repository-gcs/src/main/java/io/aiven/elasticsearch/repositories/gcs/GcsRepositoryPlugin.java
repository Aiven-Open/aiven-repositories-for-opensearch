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

import java.util.List;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import io.aiven.elasticsearch.repositories.AbstractRepositoryPlugin;

import com.google.cloud.storage.Storage;

import static io.aiven.elasticsearch.repositories.gcs.GcsClientSettings.GCS_PREFIX;

public class GcsRepositoryPlugin extends AbstractRepositoryPlugin<Storage, GcsClientSettings>  {

    public static final String REPOSITORY_TYPE = "aiven-gcs";

    public GcsRepositoryPlugin(final Settings settings) {
        super(REPOSITORY_TYPE, GCS_PREFIX, settings, new GcsSettingsProvider());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
                GcsClientSettings.PRIVATE_KEY_FILE,
                GcsClientSettings.PUBLIC_KEY_FILE,
                GcsClientSettings.CREDENTIALS_FILE_SETTING,
                GcsClientSettings.PROJECT_ID,
                GcsClientSettings.CONNECTION_TIMEOUT,
                GcsClientSettings.READ_TIMEOUT,
                GcsClientSettings.PROXY_HOST,
                GcsClientSettings.PROXY_PORT,
                GcsClientSettings.PROXY_USER_NAME,
                GcsClientSettings.PROXY_USER_PASSWORD
        );
    }

}
