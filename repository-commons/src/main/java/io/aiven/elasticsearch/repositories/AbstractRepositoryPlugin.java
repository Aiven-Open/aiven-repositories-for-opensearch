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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRepositoryPlugin<C, S extends CommonSettings.ClientSettings>
        extends Plugin implements RepositoryPlugin, ReloadablePlugin {

    public static final Logger LOGGER = LoggerFactory.getLogger(AbstractRepositoryPlugin.class);

    private final String repositoryType;

    private final RepositorySettingsProvider<C, S> repositorySettingsProvider;

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
                                       final RepositorySettingsProvider<C, S> repositorySettingsProvider) {
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

    private org.elasticsearch.repositories.blobstore.BlobStoreRepository createRepository(
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
                LOGGER.info("Reload settings for repository type: {}", repositoryType);
                repositorySettingsProvider.reload(pluginKeys);
            }
        } catch (final IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

}
