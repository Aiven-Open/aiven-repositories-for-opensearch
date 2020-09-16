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
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider;
import io.aiven.elasticsearch.repositories.io.CryptoIOProvider;
import io.aiven.elasticsearch.repositories.security.EncryptionKeyProvider;

import com.google.cloud.BatchResult;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.collect.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;

public class GcsRepositoryStorageIOProvider
        extends RepositoryStorageIOProvider<Storage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GcsRepositoryStorageIOProvider.class);

    public GcsRepositoryStorageIOProvider(final String repositoryType,
                                          final Storage client,
                                          final EncryptionKeyProvider encryptionKeyProvider) {
        super(repositoryType, client, encryptionKeyProvider);
    }

    @Override
    protected StorageIO createStorageIOFor(final String bucketName, final CryptoIOProvider cryptoIOProvider) {
        return new GcsStorageIO(bucketName, cryptoIOProvider);
    }

    private class GcsStorageIO implements StorageIO {

        private final String bucketName;

        private final CryptoIOProvider cryptoIOProvider;

        public GcsStorageIO(final String bucketName, final CryptoIOProvider cryptoIOProvider) {
            this.bucketName = bucketName;
            this.cryptoIOProvider = cryptoIOProvider;
        }

        @Override
        public boolean exists(final String blobName) throws IOException {
            try {
                final BlobId blobId = BlobId.of(bucketName, blobName);
                final Blob blob = client.get(blobId);
                return blob != null;
            } catch (final Exception e) {
                throw new BlobStoreException("Failed to check if blob [" + blobName + "] exists", e);
            }
        }

        @Override
        public InputStream read(final String blobName) throws IOException {
            final var reader = client.reader(BlobId.of(bucketName, blobName));
            return cryptoIOProvider.decryptAndDecompress(Channels.newInputStream(reader));
        }

        @Override
        public void write(final String blobName,
                          final InputStream inputStream,
                          final long blobSize,
                          final boolean failIfAlreadyExists) throws IOException {
            final var blobInfo = BlobInfo.newBuilder(bucketName, blobName).build();
            try {
                final var writeOptions = failIfAlreadyExists
                        ? new Storage.BlobWriteOption[]{Storage.BlobWriteOption.doesNotExist()}
                        : new Storage.BlobWriteOption[0];
                final var writeChannel = client.writer(blobInfo, writeOptions);
                cryptoIOProvider.compressAndEncrypt(inputStream, Channels.newOutputStream(writeChannel));
            } catch (final StorageException ex) {
                if (failIfAlreadyExists && ex.getCode() == HTTP_PRECON_FAILED) {
                    throw new FileAlreadyExistsException(blobInfo.getBlobId().getName(), null, ex.getMessage());
                }
                throw ex;
            }
        }

        @Override
        public Tuple<Integer, Long> deleteDirectories(final String path) throws IOException {
            var page = getBucket().list(Storage.BlobListOption.prefix(path));
            var deletedBlobs = 0;
            var deletedBytes = 0L;
            do {
                final var blobsToDeleteBuilder = ImmutableList.<String>builder();
                for (final var blob : page.getValues()) {
                    deletedBytes += blob.getSize();
                    blobsToDeleteBuilder.add(blob.getName());
                }
                final var blobsToDelete = blobsToDeleteBuilder.build();
                deleteFiles(blobsToDelete, true);
                deletedBlobs += blobsToDelete.size();
                page = page.getNextPage();
            } while (Objects.nonNull(page));
            return Tuple.tuple(deletedBlobs, deletedBytes);
        }

        @Override
        public void deleteFiles(final List<String> blobNames, final boolean ignoreIfNotExists) throws IOException {
            final var storageBatch = client.batch();
            final var storageExceptionHandler = new AtomicReference<StorageException>();
            blobNames.forEach(name ->
                    storageBatch
                            .delete(BlobId.of(bucketName, name))
                            .notify(new BatchResult.Callback<>() {
                                @Override
                                public void success(final Boolean result) {
                                }

                                @Override
                                public void error(final StorageException exception) {
                                    LOGGER.warn("Couldn't delete blob: {}", name, exception);
                                    if (!storageExceptionHandler.compareAndSet(null, exception)) {
                                        storageExceptionHandler.get().addSuppressed(exception);
                                    }
                                }

                            })
            );
            storageBatch.submit();
            if (Objects.nonNull(storageExceptionHandler.get())) {
                throw storageExceptionHandler.get();
            }
        }

        @Override
        public List<String> listDirectories(final String path) throws IOException {
            final var listBuilder = ImmutableList.<String>builder();
            for (final var blob : bucketBlobsIterator(path)) {
                if (blob.isDirectory()) {
                    listBuilder.add(
                            blob.getName()
                                    .substring(path.length(), blob.getName().length() - 1)
                    );
                }
            }
            return listBuilder.build();
        }

        @Override
        public Map<String, Long> listFiles(final String path, final String prefix) throws IOException {
            final var mapBuilder =
                    ImmutableMap.<String, Long>builder();
            for (final var blob : bucketBlobsIterator(path + prefix)) {
                if (!blob.isDirectory()) {
                    final var fileName = blob.getName().substring(path.length());
                    mapBuilder.put(fileName, blob.getSize());
                }
            }
            return mapBuilder.build();
        }

        private Bucket getBucket() throws IOException {
            return client.get(bucketName);
        }

        private Iterable<Blob> bucketBlobsIterator(final String path) throws IOException {
            return getBucket()
                    .list(
                            Storage.BlobListOption.currentDirectory(),
                            Storage.BlobListOption.prefix(path)
                    ).iterateAll();
        }
    }

    @Override
    public void close() throws IOException {
        //do nothing here
    }

}
