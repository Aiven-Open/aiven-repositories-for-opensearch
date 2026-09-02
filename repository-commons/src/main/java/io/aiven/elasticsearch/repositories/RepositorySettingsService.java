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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.opensearch.common.settings.Settings;
import org.opensearch.repositories.RepositoryException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.aiven.elasticsearch.repositories.CommonSettings.RepositorySettings.CLIENT_NAME;
import static org.opensearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

/**
 * Repository settings provider is used to provide settings for each repository instance, including
 * the client settings. Single instance per repository type.
 */
public abstract class RepositorySettingsService<T, S extends CommonSettings.ClientSettings> implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RepositorySettingsService.class);
    /** Map of client settings per client name. Used to create clients as needed. */
    private volatile Map<String, S> clientsSettings = Collections.emptyMap();

    private volatile int generation;

    private final ConcurrentMap<String, RepositoryStorageIOProvider<T, S>> repositoryStorages = newConcurrentMap();

    public RepositorySettingsService() {
    }

    public synchronized RepositoryStorageIOProvider.StorageIO createStorageIO(
        final String basePath,
        final String repositoryName,
        final Settings repositorySettings
    ) throws IOException {
        final String clientName = CLIENT_NAME.get(repositorySettings);
        final var ioProvider = repositoryStorages.computeIfAbsent(repositoryName,
                                                                 r -> createRepositoryStorageIOProvider());
        return ioProvider.createStorageIO(basePath, clientsSettings.get(clientName), repositorySettings);
    }

    public synchronized void reload(final Settings settings) throws IOException {
        if (settings.isEmpty()) {
            return;
        }
        clientsSettings = getEffectiveClientSettings(settings);
        generation++;
    }

    public int generation() {
        return generation;
    }

    public Map<String, RepositoryStorageIOProvider<T, S>> getRepositoryStorages() {
        return Collections.unmodifiableMap(repositoryStorages);
    }

    public Map<String, S> getClientsSettings() {
        return Collections.unmodifiableMap(clientsSettings);
    }

    public void closeRepository(final String repositoryName) throws IOException {
        final var ioProvider = repositoryStorages.remove(repositoryName);
        if (ioProvider == null) {
            throw new RepositoryException(repositoryName, "No such repository");
        }
        ioProvider.close();
    }

    @Override
    public void close() {
        synchronized (this) {
            for (final var name : repositoryStorages.keySet()) {
                try {
                    closeRepository(name);
                } catch (final IOException e) {
                    LOGGER.error("Error closing repository {}", name, e);
                }
            }
            repositoryStorages.clear();
        }
    }

    /** Load and validate client settings from the given settings. */
    protected abstract Map<String, S> getEffectiveClientSettings(Settings settings) throws IOException;

    /** Create the repository storage IO provider. */
    protected abstract RepositoryStorageIOProvider<T, S> createRepositoryStorageIOProvider();

}
