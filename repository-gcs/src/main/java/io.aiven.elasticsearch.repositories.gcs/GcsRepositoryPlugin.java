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
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import io.aiven.elasticsearch.repositories.security.EncryptionKeyProvider;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;

public class GcsRepositoryPlugin extends Plugin implements RepositoryPlugin, ReloadablePlugin {

    final GcsSettingsProvider gcsSettingsProvider;

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
                                                           final ClusterService clusterService,
                                                           final RecoverySettings recoverySettings) {
        return Map.of(GcsBlobStoreRepository.TYPE, metadata ->
                createRepository(metadata, namedXContentRegistry, clusterService, recoverySettings));
    }

    private GcsBlobStoreRepository createRepository(final RepositoryMetadata metadata,
                                                    final NamedXContentRegistry namedXContentRegistry,
                                                    final ClusterService clusterService,
                                                    final RecoverySettings recoverySettings) {
        return new GcsBlobStoreRepository(metadata, namedXContentRegistry,
                clusterService, recoverySettings, gcsSettingsProvider);
    }

    @Override
    public void reload(final Settings settings) {
        // configure plugin only in case we have
        // settings with prefix aiven
        // FIXME maybe better approach exists, with getByPrefix()
        final var hasAivenKeys = !settings.filter(key -> key.startsWith("aiven.")).isEmpty();
        if (hasAivenKeys) {
            try {
                gcsSettingsProvider.reload(settings);
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

}
