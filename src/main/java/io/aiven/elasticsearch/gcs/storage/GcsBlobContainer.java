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

package io.aiven.elasticsearch.gcs.storage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import com.google.cloud.BatchResult;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;

public class GcsBlobContainer extends AbstractBlobContainer {

    private static final Logger LOGGER = LoggerFactory.getLogger(GcsBlobContainer.class);

    static final int MAX_BLOB_SIZE = 5 * ((1 << 10) << 10);

    private final GcsBlobStore gcsBlobStore;

    private final String bucketName;

    private final String path;

    public GcsBlobContainer(final BlobPath path,
                            final GcsBlobStore gcsBlobStore,
                            final String bucketName) {
        super(path);
        this.gcsBlobStore = gcsBlobStore;
        this.bucketName = bucketName;
        this.path = path.buildAsString();
    }

    @Override
    public InputStream readBlob(final String blobName) throws IOException {
        LOGGER.info("Read blob: {}", blobName);
        final var bucketBlob = getBucket().get(blobPath(blobName));
        if (bucketBlob.getSize() < MAX_BLOB_SIZE) {
            return gcsBlobStore
                    .cryptoIOProvider()
                    .decryptAndDecompress(bucketBlob.getContent());
        } else {
            final var reader = bucketBlob.reader();
            return gcsBlobStore
                    .cryptoIOProvider()
                    .decryptAndDecompress(reader);
        }
    }

    @Override
    public void writeBlobAtomic(final String blobName,
                                final InputStream inputStream,
                                final long blobSize,
                                final boolean failIfAlreadyExists) throws IOException {
        LOGGER.info("Write blob: {} with size: {}", blobName, inputStream.available());
        writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlob(final String blobName,
                          final InputStream inputStream,
                          final long blobSize,
                          final boolean failIfAlreadyExists) throws IOException {
        final var blobInfo = BlobInfo.newBuilder(bucketName, blobPath(blobName)).build();
        try {
            if (blobSize < MAX_BLOB_SIZE) {
                writeFully(
                        blobInfo,
                        inputStream,
                        failIfAlreadyExists);
            } else {
                writeResumeable(
                        blobInfo,
                        inputStream,
                        failIfAlreadyExists);
            }
        } catch (final StorageException ex) {
            if (failIfAlreadyExists && ex.getCode() == HTTP_PRECON_FAILED) {
                throw new FileAlreadyExistsException(blobInfo.getBlobId().getName(), null, ex.getMessage());
            }
            throw ex;
        }
    }

    private void writeFully(final BlobInfo blobInfo,
                            final InputStream in,
                            final boolean failIfAlreadyExists) throws StorageException, IOException {
        final var targetOptions =
                failIfAlreadyExists
                        ? new Storage.BlobTargetOption[]{Storage.BlobTargetOption.doesNotExist()}
                        : new Storage.BlobTargetOption[0];
        gcsBlobStore
                .client()
                .create(
                        blobInfo,
                        gcsBlobStore
                                .cryptoIOProvider()
                                .compressAndEncrypt(in),
                        targetOptions);
    }

    private void writeResumeable(final BlobInfo blobInfo,
                                 final InputStream in,
                                 final boolean failIfAlreadyExists) throws StorageException, IOException {
        final var writeOptions = failIfAlreadyExists
                ? new Storage.BlobWriteOption[]{Storage.BlobWriteOption.doesNotExist()}
                : new Storage.BlobWriteOption[0];
        final var writeChannel =
                gcsBlobStore
                        .client()
                        .writer(blobInfo, writeOptions);
        gcsBlobStore
                .cryptoIOProvider()
                .compressAndEncrypt(in, writeChannel);
    }

    @Override
    public DeleteResult delete() throws IOException {
        final var deletePath = path().buildAsString();
        LOGGER.info("Delete blob: {}", deletePath);
        var deleteResult = DeleteResult.ZERO;
        var page = getBucket().list(Storage.BlobListOption.prefix(deletePath));
        do {
            var bytesToDelete = 0L;
            final var blobsToDeleteBuilder = ImmutableList.<String>builder();
            for (final var blob : page.getValues()) {
                bytesToDelete += blob.getSize();
                blobsToDeleteBuilder.add(blob.getName());
            }
            final var blobsToDelete = blobsToDeleteBuilder.build();
            deleteBlobsIgnoringIfNotExists(blobsToDelete);
            deleteResult = deleteResult.add(blobsToDelete.size(), bytesToDelete);
            page = page.getNextPage();
        } while (Objects.nonNull(page));
        return deleteResult;
    }

    private String blobPath(final String blobName) {
        return path().buildAsString() + blobName;
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(final List<String> blobNames) throws IOException {
        LOGGER.info("Delete blobs: {}", blobNames);
        if (blobNames.isEmpty()) {
            return;
        }

        final var storageBatch = gcsBlobStore.client().batch();
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
                                if (exception.getCode() != HTTP_NOT_FOUND) {
                                    if (!storageExceptionHandler.compareAndSet(null, exception)) {
                                        storageExceptionHandler.get().addSuppressed(exception);
                                    }
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
    public Map<String, BlobMetaData> listBlobs() throws IOException {
        return listBlobsByPrefix("");
    }

    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(final String blobNamePrefix) throws IOException {
        final var mapBuilder = ImmutableMap.<String, BlobMetaData>builder();
        final var prefix = path + blobNamePrefix;
        LOGGER.info("List of blobs by prefix: {}", prefix);
        bucketBlobsIterator(prefix)
                .forEach(blob -> {
                    if (!blob.isDirectory()) {
                        final var fileName = blob.getName().substring(path.length());
                        mapBuilder.put(fileName, new PlainBlobMetaData(fileName, blob.getSize()));
                    }
                });
        return mapBuilder.build();
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        final var mapBuilder = ImmutableMap.<String, BlobContainer>builder();
        bucketBlobsIterator(path)
                .forEach(blob -> {
                    if (blob.isDirectory()) {
                        final var keyName =
                                blob.getName()
                                        .substring(
                                                path.length(),
                                                blob.getName().length() - 1
                                        );
                        mapBuilder.put(
                                keyName,
                                new GcsBlobContainer(path().add(keyName), gcsBlobStore, bucketName)
                        );
                    }
                });
        return mapBuilder.build();
    }

    private Bucket getBucket() throws IOException {
        return gcsBlobStore.client().get(bucketName);
    }

    private Iterable<Blob> bucketBlobsIterator(final String path) throws IOException {
        return getBucket()
                .list(
                        Storage.BlobListOption.currentDirectory(),
                        Storage.BlobListOption.prefix(path)
                ).iterateAll();
    }


}
