/*
 * Copyright 2021 Aiven Oy
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
import java.util.Objects;

import org.opensearch.common.settings.Settings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ClientProvider<C, S extends CommonSettings.ClientSettings> implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientProvider.class);

    private final Object lock = new Object();

    private volatile Settings previousRepositorySettings;

    protected volatile C client;

    public C buildClientIfNeeded(final S clientSettings, final Settings repositorySettings) throws IOException {
        synchronized (lock) {
            // Check if client name has changed - this is critical for credential resolution
            final var currentClientName = getClientName(repositorySettings);
            final var previousClientName = previousRepositorySettings != null 
                ? getClientName(previousRepositorySettings) : null;
            
            if (Objects.isNull(client)) {
                // First time: create client
                client = buildClient(clientSettings, repositorySettings);
                previousRepositorySettings = repositorySettings;
            } else if (!Objects.equals(currentClientName, previousClientName) 
                       || !previousRepositorySettings.equals(repositorySettings)) {
                // Client name changed OR other settings changed: recreate client
                LOGGER.debug("Recreating client: client name changed from '{}' to '{}' or other settings changed", 
                           previousClientName, currentClientName);
                closeClient();
                client = buildClient(clientSettings, repositorySettings);
                previousRepositorySettings = repositorySettings;
            }
            // If client name is the same AND other settings are the same: reuse existing client
        }
        return client;
    }
    
    /**
     * Extracts the client name from repository settings.
     * This method is overridden by specific implementations to handle their client parameter.
     */
    protected String getClientName(final Settings repositorySettings) {
        // Default implementation - subclasses should override this
        return null;
    }

    @Override
    public void close() throws IOException {
        synchronized (lock) {
            closeClient();
            client = null;
        }
    }

    protected abstract void closeClient();

    protected abstract C buildClient(final S clientSettings, final Settings repositorySettings);

}
