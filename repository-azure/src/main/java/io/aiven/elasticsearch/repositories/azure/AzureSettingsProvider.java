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

package io.aiven.elasticsearch.repositories.azure;

import java.io.IOException;

import org.opensearch.common.settings.Settings;

import io.aiven.elasticsearch.repositories.Permissions;
import io.aiven.elasticsearch.repositories.RepositorySettingsProvider;
import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider;

import com.azure.storage.blob.BlobServiceClient;

public class AzureSettingsProvider extends RepositorySettingsProvider<BlobServiceClient, AzureClientSettings> {

    @Override
    protected RepositoryStorageIOProvider<BlobServiceClient, AzureClientSettings> createRepositoryStorageIOProvider(
            final Settings settings) throws IOException {
        return Permissions.doPrivileged(() -> {
            final var azureClientSettings = AzureClientSettings.create(settings);
            return Permissions.doPrivileged(() ->
                    new AzureRepositoryStorageIOProvider(azureClientSettings));
        });
    }

}
