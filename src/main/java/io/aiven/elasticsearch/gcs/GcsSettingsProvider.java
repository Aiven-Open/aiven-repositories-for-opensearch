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

package io.aiven.elasticsearch.gcs;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.aiven.elasticsearch.storage.security.EncryptionKeyProvider;

import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Strings;
import com.google.common.net.HttpHeaders;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.LazyInitializable;

public class GcsSettingsProvider {

    static final String HTTP_USER_AGENT = "Aiven GCS encrypted repository";
    private final Lock lock = new ReentrantLock();
    private LazyInitializable<Storage, IOException> cachedClient =
            new LazyInitializable<>(() -> createGcsClient(GcsStorageSettings.load(Settings.EMPTY)));
    private LazyInitializable<EncryptionKeyProvider, IOException> cachedEncryptionKeyProvider =
            new LazyInitializable<>(() -> EncryptionKeyProvider.of(Settings.EMPTY));

    public Storage gcsClient() throws IOException {
        return cachedClient.getOrCompute();
    }

    public EncryptionKeyProvider encryptionKeyProvider() throws IOException {
        return cachedEncryptionKeyProvider.getOrCompute();
    }

    public void reload(final Settings settings) throws IOException {
        lock.lock();
        try {
            final var gcsSettings = GcsStorageSettings.load(settings);
            cachedClient.reset();
            cachedClient = new LazyInitializable<>(() -> createGcsClient(gcsSettings));
            cachedEncryptionKeyProvider = new LazyInitializable<>(() -> EncryptionKeyProvider.of(settings));
        } finally {
            lock.unlock();
        }
    }

    private Storage createGcsClient(final GcsStorageSettings gcsStorageSettings) {

        final StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder();
        if (!Strings.isNullOrEmpty(gcsStorageSettings.projectId())) {
            storageOptionsBuilder.setProjectId(gcsStorageSettings.projectId());
        }
        storageOptionsBuilder
                .setTransportOptions(
                        HttpTransportOptions.newBuilder()
                                .setConnectTimeout(gcsStorageSettings.connectionTimeout())
                                .setReadTimeout(gcsStorageSettings.readTimeout())
                                .build())
                .setHeaderProvider(() -> Map.of(HttpHeaders.USER_AGENT, HTTP_USER_AGENT))
                .setCredentials(gcsStorageSettings.gcsCredentials());

        return storageOptionsBuilder.build().getService();
    }

}
