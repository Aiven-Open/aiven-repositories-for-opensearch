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

import io.aiven.elasticsearch.repositories.AbstractRepositoryPlugin;

import com.google.cloud.storage.Storage;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

public class GcsRepositoryPlugin extends AbstractRepositoryPlugin<Storage>  {

    public static final String REPOSITORY_TYPE = "aiven-gcs";

    public GcsRepositoryPlugin(final Settings settings) {
        super(REPOSITORY_TYPE, settings, new GcsSettingsProvider());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
                GcsStorageSettings.PRIVATE_KEY_FILE,
                GcsStorageSettings.PUBLIC_KEY_FILE,
                GcsStorageSettings.CREDENTIALS_FILE_SETTING,
                GcsStorageSettings.PROJECT_ID,
                GcsStorageSettings.CONNECTION_TIMEOUT,
                GcsStorageSettings.READ_TIMEOUT,
                GcsStorageSettings.PROXY_HOST,
                GcsStorageSettings.PROXY_PORT,
                GcsStorageSettings.PROXY_USER_NAME,
                GcsStorageSettings.PROXY_USER_PASSWORD
        );
    }

}
