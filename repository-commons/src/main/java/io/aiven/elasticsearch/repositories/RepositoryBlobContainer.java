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
import java.io.InputStream;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStoreException;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.support.AbstractBlobContainer;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider.StorageIO;

public class RepositoryBlobContainer extends AbstractBlobContainer {

    private final Logger logger = LoggerFactory.getLogger(RepositoryBlobContainer.class);

    private final StorageIO storageIO;

    public RepositoryBlobContainer(final BlobPath path, final StorageIO storageIO) {
        super(path);
        this.storageIO = storageIO;
    }

    @Override
    public boolean blobExists(final String blobName) throws IOException {
        try {
            logger.info("Check existing blob: {}", blobPath(blobName));
            return storageIO.exists(blobPath(blobName));
        } catch (final Exception e) {
            throw new BlobStoreException("Failed to check if blob [" + blobName + "] exists", e);
        }
    }

    @Override
    public InputStream readBlob(final String blobName) throws IOException {
        logger.info("Read blob: {}", blobPath(blobName));
        return storageIO.read(blobPath(blobName));
    }

    @Override
    public InputStream readBlob(final String blobName,
                                final long position,
                                final long length) throws IOException {
        throw new UnsupportedOperationException("Couldn't decrypt by position");
    }

    @Override
    public void writeBlobAtomic(final String blobName,
                                final InputStream inputStream,
                                final long blobSize,
                                final boolean failIfAlreadyExists) throws IOException {
        writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlob(final String blobName,
                          final InputStream inputStream,
                          final long blobSize,
                          final boolean failIfAlreadyExists) throws IOException {
        logger.info("Write blob: {}", blobPath(blobName));
        storageIO.write(blobPath(blobName), inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public DeleteResult delete() throws IOException {
        logger.info("Delete: {}", path().buildAsString());
        final var result =
                storageIO.deleteDirectories(path().buildAsString());
        return new DeleteResult(result.v1(), result.v2());
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(final List<String> blobNames) throws IOException {
        if (blobNames.isEmpty()) {
            return;
        }
        final var blobLists =
                blobNames.stream()
                        .map(this::blobPath)
                        .collect(Collectors.toUnmodifiableList());
        logger.info("Delete blobs: {}", blobLists);
        storageIO.deleteFiles(blobLists, true);
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        logger.info("Children for: {}", path().buildAsString());
        return storageIO.listDirectories(path().buildAsString())
                .stream()
                .map(d -> new AbstractMap.SimpleEntry<String, BlobContainer>(
                        d,
                        new RepositoryBlobContainer(path().add(d), storageIO))
                )
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        return listBlobsByPrefix("");
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(final String blobNamePrefix) throws IOException {
        logger.info("List of blobs by {}", blobNamePrefix);
        return storageIO
                .listFiles(path().buildAsString(), blobNamePrefix)
                .entrySet().stream()
                .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), new PlainBlobMetadata(e.getKey(), e.getValue())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private String blobPath(final String blobName) {
        return path().buildAsString() + blobName;
    }

}
