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
import java.util.Objects;

import org.opensearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RepositorySettingsProvider<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RepositorySettingsProvider.class);

    private volatile RepositoryStorageIOProvider<T> repositoryStorageIOProvider;

    public synchronized RepositoryStorageIOProvider<T> repositoryStorageIOProvider() throws IOException {
        if (Objects.isNull(repositoryStorageIOProvider)) {
            throw new IOException("Cloud storage client haven't been configured");
        }
        return repositoryStorageIOProvider;
    }

    public synchronized void reload(final String repositoryType, final Settings settings) throws IOException {
        if (settings.isEmpty()) {
            return;
        }

        try {
            LOGGER.info("Reload settings for repository type: {}", repositoryType);
            this.repositoryStorageIOProvider = createRepositoryStorageIOProvider(settings);
        } catch (final Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    protected abstract RepositoryStorageIOProvider<T> createRepositoryStorageIOProvider(final Settings settings)
            throws IOException;

}
