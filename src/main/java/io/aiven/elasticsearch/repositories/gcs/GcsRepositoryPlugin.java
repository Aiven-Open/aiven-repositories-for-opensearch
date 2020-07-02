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
import java.util.Map;

import io.aiven.elasticsearch.repositories.security.EncryptionKeyProvider;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;

public class GcsRepositoryPlugin extends Plugin implements RepositoryPlugin, ReloadablePlugin {

    private final GcsSettingsProvider gcsSettingsProvider;

    public GcsRepositoryPlugin(final Settings settings) {
        this.gcsSettingsProvider = new GcsSettingsProvider();
        reload(settings);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            GcsStorageSettings.CREDENTIALS_FILE_SETTING,
            GcsStorageSettings.PROJECT_ID,
            GcsStorageSettings.CONNECTION_TIMEOUT,
            GcsStorageSettings.READ_TIMEOUT,
            EncryptionKeyProvider.PUBLIC_KEY_FILE,
            EncryptionKeyProvider.PRIVATE_KEY_FILE
        );
    }

    public Map<String, Repository.Factory> getRepositories(final Environment env,
                                                           final NamedXContentRegistry namedXContentRegistry,
                                                           final ClusterService clusterService) {
        return Map.of(
            GcsBlobStoreRepository.TYPE,
            metadata -> createBlobStore(metadata, namedXContentRegistry, clusterService)
        );
    }

    private GcsBlobStoreRepository createBlobStore(final RepositoryMetadata metadata,
                                                   final NamedXContentRegistry namedXContentRegistry,
                                                   final ClusterService clusterService) {
        return new GcsBlobStoreRepository(metadata, namedXContentRegistry, clusterService, gcsSettingsProvider);
    }

    @Override
    public void reload(final Settings settings) {
        gcsSettingsProvider.reload(settings);
    }

}
