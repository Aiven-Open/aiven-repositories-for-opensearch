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

package io.aiven.elasticsearch.repositories;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.Security;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ReloadablePlugin;
import org.opensearch.plugins.RepositoryPlugin;
import org.opensearch.repositories.Repository;

public abstract class AbstractRepositoryPlugin<C>
        extends Plugin implements RepositoryPlugin, ReloadablePlugin {

    private final String repositoryType;

    private final RepositorySettingsProvider<C> repositorySettingsProvider;

    private final Set<String> pluginSettingKeys;

    static {
        try {
            Permissions.doPrivileged(() -> Security.addProvider(new BouncyCastleProvider()));
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected AbstractRepositoryPlugin(final String repositoryType,
                                       final Settings settings,
                                       final RepositorySettingsProvider<C> repositorySettingsProvider) {
        this.repositoryType = repositoryType;
        this.repositorySettingsProvider = repositorySettingsProvider;
        this.pluginSettingKeys = getSettings().stream().map(Setting::getKey).collect(Collectors.toSet());
        reload(settings);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(final Environment env,
                                                           final NamedXContentRegistry namedXContentRegistry,
                                                           final ClusterService clusterService,
                                                           final RecoverySettings recoverySettings) {
        return Map.of(repositoryType, metadata ->
                createRepository(metadata, namedXContentRegistry, clusterService, recoverySettings));
    }

    private org.opensearch.repositories.blobstore.BlobStoreRepository createRepository(
            final RepositoryMetadata metadata, final NamedXContentRegistry namedXContentRegistry,
            final ClusterService clusterService, final RecoverySettings recoverySettings) {
        return new BlobStoreRepository<>(metadata, namedXContentRegistry,
                clusterService, recoverySettings, repositorySettingsProvider);
    }

    @Override
    public void reload(final Settings settings) {
        try {
            final var pluginKeys = settings.filter(pluginSettingKeys::contains);
            if (!pluginKeys.isEmpty()) {
                repositorySettingsProvider.reload(repositoryType, pluginKeys);
            }
        } catch (final IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

}
