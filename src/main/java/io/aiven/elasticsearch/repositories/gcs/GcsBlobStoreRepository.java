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

import javax.crypto.SecretKey;

import java.io.IOException;
import java.util.Objects;

import io.aiven.elasticsearch.repositories.Permissions;
import io.aiven.elasticsearch.repositories.io.CryptoIOProvider;
import io.aiven.elasticsearch.repositories.metadata.EncryptedRepositoryMetadata;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.common.base.Strings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcsBlobStoreRepository extends BlobStoreRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(GcsBlobStoreRepository.class);

    public static final Setting<String> BASE_PATH =
            Setting.simpleString(
                    "base_path",
                    Setting.Property.NodeScope,
                    Setting.Property.Dynamic
            );
    public static final String TYPE = "aiven-gcs";
    static final Setting<ByteSizeValue> CHUNK_SIZE =
            Setting.byteSizeSetting(
                    "chunk_size",
                    new ByteSizeValue(100, ByteSizeUnit.MB),
                    new ByteSizeValue(1, ByteSizeUnit.BYTES),
                    new ByteSizeValue(100, ByteSizeUnit.MB),
                    Setting.Property.NodeScope
            );

    static final Setting<String> BUCKET_NAME =
            Setting.simpleString(
                    "bucket_name",
                    Setting.Property.NodeScope,
                    Setting.Property.Dynamic
            );

    public static final String REPOSITORY_METADATA_FILE_NAME = "repository_metadata.json";

    private final GcsSettingsProvider gcsSettingsProvider;

    private final BlobPath basePath;

    private final String repositoryMetadataFilePath;

    public GcsBlobStoreRepository(final org.elasticsearch.cluster.metadata.RepositoryMetadata metadata,
                                  final NamedXContentRegistry namedXContentRegistry,
                                  final ClusterService clusterService,
                                  final GcsSettingsProvider gcsSettingsProvider) {
        //we forbid compression on elastic search level ...
        super(metadata, false, namedXContentRegistry, clusterService);
        this.gcsSettingsProvider = gcsSettingsProvider;
        this.basePath = buildBasePath(metadata);
        this.repositoryMetadataFilePath = this.basePath().buildAsString() + REPOSITORY_METADATA_FILE_NAME;
        checkSettings(BUCKET_NAME);
    }

    static BlobPath buildBasePath(final org.elasticsearch.cluster.metadata.RepositoryMetadata repositoryMetaData) {
        final String basePath = BASE_PATH.get(repositoryMetaData.settings());
        var blobPath = BlobPath.cleanPath();
        if (!Strings.isNullOrEmpty(basePath)) {
            final var paths = basePath.split("/");
            for (final String elem : paths) {
                blobPath = blobPath.add(elem);
            }
        }
        return blobPath;
    }

    private void checkSettings(final Setting<String> setting) {
        if (!setting.exists(metadata.settings())) {
            throw new RepositoryException(TYPE, setting.getKey() + " hasn't been defined");
        }
    }

    @Override
    public BlobPath basePath() {
        return basePath;
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return CHUNK_SIZE.get(metadata.settings());
    }

    protected String bucketName() {
        return BUCKET_NAME.get(metadata.settings());
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        final var encryptionKey = createOrRestoreEncryptionKey(gcsSettingsProvider.gcsClient());
        return new GcsBlobStore(
                gcsSettingsProvider,
                new CryptoIOProvider(encryptionKey),
                bucketName()
        );
    }

    private SecretKey createOrRestoreEncryptionKey(final Storage storage) throws IOException {
        return Permissions.doPrivileged(() -> {
            final var encryptedKeyBlobId = BlobId.of(bucketName(), repositoryMetadataFilePath);
            final var encryptedKeyBlob = storage.get(encryptedKeyBlobId);
            final var repositoryMetadata = new EncryptedRepositoryMetadata(gcsSettingsProvider.encryptionKeyProvider());
            final SecretKey encryptionKey;
            if (Objects.nonNull(encryptedKeyBlob) && encryptedKeyBlob.exists()) {
                LOGGER.info("Restore encryption key for repository. Path: {}", encryptedKeyBlobId.getName());
                encryptionKey = repositoryMetadata.deserialize(encryptedKeyBlob.getContent());
            } else {
                LOGGER.info("Create new encryption key for repository. Path: {}", encryptedKeyBlobId.getName());
                encryptionKey = gcsSettingsProvider.encryptionKeyProvider().createKey();
                storage.create(
                        BlobInfo.newBuilder(encryptedKeyBlobId).build(), repositoryMetadata.serialize(encryptionKey)
                );
            }
            return encryptionKey;
        });
    }

}
