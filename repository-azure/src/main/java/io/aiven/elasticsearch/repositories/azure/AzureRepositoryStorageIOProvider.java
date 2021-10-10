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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.opensearch.common.blobstore.BlobStoreException;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import io.aiven.elasticsearch.repositories.CommonSettings;
import io.aiven.elasticsearch.repositories.Permissions;
import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider;
import io.aiven.elasticsearch.repositories.io.CryptoIOProvider;
import io.aiven.elasticsearch.repositories.security.EncryptionKeyProvider;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.ListBlobsOptions;

public class AzureRepositoryStorageIOProvider
        extends RepositoryStorageIOProvider<BlobServiceClient, AzureClientSettings> {

    static final Setting<String> CONTAINER_NAME = Setting.simpleString("container_name");

    public AzureRepositoryStorageIOProvider(final AzureClientSettings clientSettings,
                                            final EncryptionKeyProvider encryptionKeyProvider) {
        super(new AzureClientProvider(), clientSettings, encryptionKeyProvider);
    }

    @Override
    protected StorageIO createStorageIOFor(final BlobServiceClient client,
                                           final Settings repositorySettings,
                                           final CryptoIOProvider cryptoIOProvider) {
        try {
            CommonSettings.RepositorySettings
                    .checkSettings(AzureRepositoryPlugin.REPOSITORY_TYPE, CONTAINER_NAME, repositorySettings);
            final var blobContainer = client.createBlobContainer(CONTAINER_NAME.get(repositorySettings));
            return Permissions.doPrivileged(() -> new AzureStorageIO(blobContainer, cryptoIOProvider));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static class AzureStorageIO implements StorageIO {

        private final CryptoIOProvider cryptoIOProvider;

        private final BlobContainerClient blobContainerClient;

        AzureStorageIO(final BlobContainerClient blobContainerClient, final CryptoIOProvider cryptoIOProvider) {
            this.cryptoIOProvider = cryptoIOProvider;
            this.blobContainerClient = blobContainerClient;
        }

        @Override
        public boolean exists(final String blobName) throws IOException {
            try {
                return Permissions.doPrivileged(() ->
                        blobContainerClient.getBlobClient(blobName).exists());
            } catch (final Exception e) {
                throw new BlobStoreException("Failed to check if blob [" + blobName + "] exists", e);
            }
        }

        @Override
        public InputStream read(final String blobName) throws IOException {
            return Permissions.doPrivileged(() ->
                    cryptoIOProvider.decryptAndDecompress(
                            blobContainerClient.getBlobClient(blobName).openInputStream()));
        }

        @Override
        public void write(final String blobName,
                          final InputStream inputStream,
                          final long blobSize,
                          final boolean failIfAlreadyExists) throws IOException {
            try {
                Permissions.doPrivileged(() -> {
                    final var azureOutputStream =
                            blobContainerClient
                                    .getBlobClient(blobName)
                                    .getBlockBlobClient()
                                    .getBlobOutputStream(true); //always overwrite
                    cryptoIOProvider.compressAndEncrypt(
                            inputStream,
                            new OutputStream() {
                                @Override
                                public void write(final int b) throws IOException {
                                    write(new byte[]{(byte) b});
                                }

                                @Override
                                public void write(final byte[] b) throws IOException {
                                    Permissions.doPrivileged(() -> azureOutputStream.write(b));
                                }

                                @Override
                                public void write(final byte[] b, final int off, final int len) throws IOException {
                                    Permissions.doPrivileged(() -> azureOutputStream.write(b, off, len));
                                }

                                @Override
                                public void flush() throws IOException {
                                    Permissions.doPrivileged(azureOutputStream::flush);
                                }

                                @Override
                                public void close() throws IOException {
                                    Permissions.doPrivileged(azureOutputStream::close);
                                }

                            });
                });
            } catch (final Exception e) { //use just exception ... it could throw IllegalArgumentException
                throw new IOException(e);
            }
        }

        @Override
        public Tuple<Integer, Long> deleteDirectories(final String path) throws IOException {
            try {
                return Permissions.doPrivileged(() -> {
                    final var files =
                            blobContainerClient.listBlobsByHierarchy(path);
                    var bytesCounter = 0L;
                    final var filesList = new ArrayList<String>();
                    for (final var blobItem : files) {
                        filesList.add(blobItem.getName());
                        bytesCounter += Optional.ofNullable(blobItem.getProperties())
                                .map(BlobItemProperties::getContentLength)
                                .orElse(0L);
                    }
                    deleteFiles(filesList, true);
                    return Tuple.tuple(filesList.size(), bytesCounter);
                });
            } catch (final Exception e) {
                throw new IOException("Couldn't delete directories for path: " + path, e);
            }
        }

        @Override
        public void deleteFiles(final List<String> blobNames,
                                final boolean ignoreIfNotExists) throws IOException {
            try {
                Permissions.doPrivileged(() -> blobNames.forEach(
                        blobName -> blobContainerClient.getBlobClient(blobName).delete()));
            } catch (final BlobStoreException e) {
                throw new IOException("Couldn't delete objects: " + blobNames, e);
            }
        }

        @Override
        public List<String> listDirectories(final String path) throws IOException {
            try {
                return blobContainerClient
                        .listBlobsByHierarchy(path)
                        .stream()
                        .filter(BlobItem::isPrefix)
                        .map(b -> b.getName().substring(path.length()))
                        .collect(Collectors.toList());
            } catch (final Exception e) {
                throw new IOException("Couldn't get list of directories for " + path, e);
            }
        }

        @Override
        public Map<String, Long> listFiles(final String path, final String prefix) throws IOException {
            try {
                final var fullPath = path + prefix;

                final Function<BlobItem, String> mapBlobItemName = b ->
                        b.getName().substring(fullPath.length());
                final Function<BlobItem, Long> mapBlobItemProperties = b ->
                        Optional.ofNullable(b.getProperties())
                                .map(BlobItemProperties::getContentLength)
                                .orElse(0L);

                return blobContainerClient
                        .listBlobsByHierarchy(
                                "/",
                                new ListBlobsOptions().setPrefix(fullPath),
                                null
                        )
                        .stream()
                        .filter(b -> Objects.nonNull(b.isPrefix()) && b.isPrefix())
                        .collect(Collectors.toMap(mapBlobItemName, mapBlobItemProperties));
            } catch (final Exception e) {
                throw new IOException("Couldn't get list of files for " + path + " and prefix " + prefix, e);
            }
        }
    }

}
