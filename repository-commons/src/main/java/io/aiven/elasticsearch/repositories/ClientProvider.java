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

public abstract class ClientProvider<C, S extends CommonSettings.ClientSettings> implements Closeable {

    private final Object lock = new Object();

    private volatile Settings previousRepositorySettings;

    protected volatile C client;

    public C buildClientIfNeeded(final S clientSettings, final Settings repositorySettings) throws IOException {
        synchronized (lock) {
            if (Objects.isNull(client)) {
                client = buildClient(clientSettings, repositorySettings);
                previousRepositorySettings = repositorySettings;
            } else if (!previousRepositorySettings.equals(repositorySettings)) {
                closeClient();
                client = buildClient(clientSettings, repositorySettings);
                previousRepositorySettings = repositorySettings;
            }
        }
        return client;
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
