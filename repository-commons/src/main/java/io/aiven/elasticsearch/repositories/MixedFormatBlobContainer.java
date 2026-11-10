/*
 * Copyright 2026 Aiven Oy
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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.EncryptedBlobMetadata;
import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.common.crypto.EncryptedHeaderContentSupplier;

/**
 * Blob container of a repository with {@code crypto_settings} whose base path also holds blobs written before it had
 * them. Writes always go through core encryption. Each read picks the format from the first bytes of the blob.
 */
public class MixedFormatBlobContainer implements BlobContainer {

    // Blobs written through crypto_settings start with an AWS Encryption SDK message format version 2 header:
    // version 0x02, suite 0x0478, a 32-byte message id, an empty encryption context and one encrypted data key.
    // Blobs encrypted with the repository key start with a random 16-byte IV, so they match these 7 fixed bytes
    // with probability 2^-56.
    static final int HEADER_PREFIX_LENGTH = 39;

    private final BlobContainer encryptedBlobContainer;

    private final BlobContainer storedBlobContainer;

    private final BlobContainer repositoryKeyBlobContainer;

    private final CryptoHandler<?, ?> cryptoHandler;

    private final Function<BlobPath, BlobContainer> childContainers;

    /**
     * @param encryptedBlobContainer     core encryption over {@code storedBlobContainer}
     * @param storedBlobContainer        the bytes as stored
     * @param repositoryKeyBlobContainer decrypts blobs with the key from {@code repository_metadata.json}
     * @param cryptoHandler              the handler of {@code encryptedBlobContainer}
     * @param childContainers            creates the container for a child path
     */
    public MixedFormatBlobContainer(final BlobContainer encryptedBlobContainer,
                                    final BlobContainer storedBlobContainer,
                                    final BlobContainer repositoryKeyBlobContainer,
                                    final CryptoHandler<?, ?> cryptoHandler,
                                    final Function<BlobPath, BlobContainer> childContainers) {
        this.encryptedBlobContainer = encryptedBlobContainer;
        this.storedBlobContainer = storedBlobContainer;
        this.repositoryKeyBlobContainer = repositoryKeyBlobContainer;
        this.cryptoHandler = cryptoHandler;
        this.childContainers = childContainers;
    }

    @Override
    public BlobPath path() {
        return encryptedBlobContainer.path();
    }

    @Override
    public boolean blobExists(final String blobName) throws IOException {
        return encryptedBlobContainer.blobExists(blobName);
    }

    @Override
    public InputStream readBlob(final String blobName) throws IOException {
        if (hasCryptoSettingsFormat(blobName)) {
            return encryptedBlobContainer.readBlob(blobName);
        }
        return repositoryKeyBlobContainer.readBlob(blobName);
    }

    @Override
    public InputStream readBlob(final String blobName, final long position, final long length) throws IOException {
        if (hasCryptoSettingsFormat(blobName)) {
            return encryptedBlobContainer.readBlob(blobName, position, length);
        }
        return repositoryKeyBlobContainer.readBlob(blobName, position, length);
    }

    @Override
    public long readBlobPreferredLength() {
        return encryptedBlobContainer.readBlobPreferredLength();
    }

    @Override
    public void writeBlob(final String blobName,
                          final InputStream inputStream,
                          final long blobSize,
                          final boolean failIfAlreadyExists) throws IOException {
        encryptedBlobContainer.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlobWithMetadata(final String blobName,
                                      final InputStream inputStream,
                                      final long blobSize,
                                      final boolean failIfAlreadyExists,
                                      final Map<String, String> metadata) throws IOException {
        encryptedBlobContainer.writeBlobWithMetadata(blobName, inputStream, blobSize, failIfAlreadyExists, metadata);
    }

    @Override
    public void writeBlobWithMetadata(final String blobName,
                                      final InputStream inputStream,
                                      final long blobSize,
                                      final boolean failIfAlreadyExists,
                                      final Map<String, String> metadata,
                                      final CryptoMetadata cryptoMetadata) throws IOException {
        encryptedBlobContainer.writeBlobWithMetadata(
            blobName, inputStream, blobSize, failIfAlreadyExists, metadata, cryptoMetadata);
    }

    @Override
    public void writeBlobAtomic(final String blobName,
                                final InputStream inputStream,
                                final long blobSize,
                                final boolean failIfAlreadyExists) throws IOException {
        encryptedBlobContainer.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public DeleteResult delete() throws IOException {
        return encryptedBlobContainer.delete();
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(final List<String> blobNames) throws IOException {
        encryptedBlobContainer.deleteBlobsIgnoringIfNotExists(blobNames);
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        return listBlobsByPrefix("");
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(final String blobNamePrefix) throws IOException {
        return storedBlobContainer.listBlobsByPrefix(blobNamePrefix).values().stream()
            .collect(Collectors.toMap(BlobMetadata::name, this::blobMetadata));
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        return storedBlobContainer.children().keySet().stream()
            .collect(Collectors.toMap(Function.identity(), name -> childContainers.apply(path().add(name))));
    }

    private BlobMetadata blobMetadata(final BlobMetadata stored) {
        return new BlobMetadata() {
            @Override
            public String name() {
                return stored.name();
            }

            @Override
            public long length() {
                try {
                    if (hasCryptoSettingsFormat(stored.name())) {
                        return new EncryptedBlobMetadata<>(stored, cryptoHandler, headerSupplier(stored.name())).length();
                    }
                } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
                // The plaintext length of a blob encrypted with the repository key is only known after
                // decompressing all of it.
                return stored.length();
            }
        };
    }

    private EncryptedHeaderContentSupplier headerSupplier(final String blobName) {
        return (start, end) -> {
            final var length = Math.toIntExact(end - start + 1);
            final var header = new byte[length];
            try (final var in = storedBlobContainer.readBlob(blobName, start, length)) {
                in.readNBytes(header, 0, length);
            }
            return header;
        };
    }

    private boolean hasCryptoSettingsFormat(final String blobName) throws IOException {
        final byte[] prefix;
        try (final var in = storedBlobContainer.readBlob(blobName, 0, HEADER_PREFIX_LENGTH)) {
            prefix = in.readNBytes(HEADER_PREFIX_LENGTH);
        }
        if (prefix.length < HEADER_PREFIX_LENGTH) {
            return false;
        }
        final var header = ByteBuffer.wrap(prefix);
        return header.get(0) == 0x02
            && header.getShort(1) == 0x0478
            && header.getInt(35) == 1;
    }
}
