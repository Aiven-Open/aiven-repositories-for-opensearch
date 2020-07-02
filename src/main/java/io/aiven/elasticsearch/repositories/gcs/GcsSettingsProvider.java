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
import java.util.Map;

import io.aiven.elasticsearch.repositories.security.EncryptionKeyProvider;

import com.google.cloud.Tuple;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Strings;
import com.google.common.net.HttpHeaders;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.LazyInitializable;

public class GcsSettingsProvider {

    static final String HTTP_USER_AGENT = "Aiven Google Cloud Storage repository";

    private volatile LazyInitializable<Tuple<Storage, EncryptionKeyProvider>, IOException> cachedSettings =
            new LazyInitializable<>(() -> cachedSettingsFrom(Settings.EMPTY));

    public Storage gcsClient() throws IOException {
        return cachedSettings.getOrCompute().x();
    }

    public EncryptionKeyProvider encryptionKeyProvider() throws IOException {
        return cachedSettings.getOrCompute().y();
    }

    public void reload(final Settings settings) {
        cachedSettings = new LazyInitializable<>(() -> cachedSettingsFrom(settings));
    }

    private Tuple<Storage, EncryptionKeyProvider> cachedSettingsFrom(final Settings settings) throws IOException {
        return Tuple.of(
                createGcsClient(GcsStorageSettings.load(settings)),
                EncryptionKeyProvider.of(settings)
        );
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
