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

package io.aiven.elasticsearch.repositories.s3;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.aiven.elasticsearch.repositories.Permissions;
import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider;
import io.aiven.elasticsearch.repositories.io.CryptoIOProvider;
import io.aiven.elasticsearch.repositories.security.EncryptionKeyProvider;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.opensearch.common.blobstore.BlobStoreException;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3RepositoryStorageIOProvider extends RepositoryStorageIOProvider<AmazonS3> {

    static final Setting<String> BUCKET_NAME =
            Setting.simpleString(
                    "bucket_name",
                    Setting.Property.NodeScope,
                    Setting.Property.Dynamic);

    static final Setting<ByteSizeValue> MULTIPART_UPLOAD_PART_SIZE =
            Setting.byteSizeSetting(
                    "multipart_upload_part_size",
                    new ByteSizeValue(100, ByteSizeUnit.MB),
                    new ByteSizeValue(100, ByteSizeUnit.MB),
                    new ByteSizeValue(5, ByteSizeUnit.GB),
                    Setting.Property.NodeScope,
                    Setting.Property.Dynamic);

    public S3RepositoryStorageIOProvider(final AmazonS3 client,
                                         final EncryptionKeyProvider encryptionKeyProvider) {
        super(client, encryptionKeyProvider);
    }

    @Override
    protected StorageIO createStorageIOFor(final Settings repositorySettings,
                                           final CryptoIOProvider cryptoIOProvider) {
        checkSettings(S3RepositoryPlugin.REPOSITORY_TYPE, BUCKET_NAME, repositorySettings);
        final var bucketName = BUCKET_NAME.get(repositorySettings);
        final var multipartUploadPartSize =
                Math.toIntExact(MULTIPART_UPLOAD_PART_SIZE.get(repositorySettings).getBytes());
        return new S3StorageIO(bucketName, multipartUploadPartSize, cryptoIOProvider);
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(client)) {
            client.shutdown();
        }
    }

    protected class S3StorageIO implements StorageIO {

        private final Logger logger = LoggerFactory.getLogger(S3StorageIO.class);

        private final int partSize;

        private final String bucketName;

        private final CryptoIOProvider cryptoIOProvider;

        private S3StorageIO(final String bucketName, final int partSize, final CryptoIOProvider cryptoIOProvider) {
            this.bucketName = bucketName;
            this.partSize = partSize;
            this.cryptoIOProvider = cryptoIOProvider;
        }

        @Override
        public boolean exists(final String blobName) throws IOException {
            try {
                return Permissions.doPrivileged(() -> client.doesObjectExist(bucketName, blobName));
            } catch (final Exception e) {
                throw new BlobStoreException("Failed to check if blob [" + blobName + "] exists", e);
            }
        }

        @Override
        public InputStream read(final String blobName) throws IOException {
            return Permissions.doPrivileged(() -> {
                final S3ObjectInputStream objectContent;
                try {
                    objectContent = client.getObject(bucketName, blobName).getObjectContent();
                } catch (final AmazonClientException e) {
                    throw new IOException("Couldn't read blob " + blobName, e);
                }
                return cryptoIOProvider.decryptAndDecompress(objectContent);
            });
        }

        @Override
        public void write(final String blobName,
                          final InputStream inputStream,
                          final long blobSize,
                          final boolean failIfAlreadyExists) throws IOException {
            try {
                Permissions.doPrivileged(() ->
                        cryptoIOProvider.compressAndEncrypt(
                                inputStream,
                                new S3OutputStream(bucketName, blobName, partSize, client)
                        )
                );
            } catch (final AmazonClientException e) {
                throw new IOException("Couldn't upload blob with name: " + blobName, e);
            }
        }

        @Override
        public Tuple<Integer, Long> deleteDirectories(final String path) throws IOException {
            final List<Tuple<String, Long>> listOfFiles;
            try {
                //we need only prefix here since it is impossible to distinguish between "files" and "directories"
                //in AWS SDK e.g. paths like:
                // - /aaaa/bbb/ccc/file
                // - /aaa/bbb/file
                // with request which contains delimiter and without returns diff result, and ES API doesn't use
                // recursive calls to get full list of files by path/sub-path
                listOfFiles = Permissions.doPrivileged(() ->
                        list(new ListObjectsV2Request().withBucketName(bucketName).withPrefix(path)))
                        .stream()
                        .flatMap(l -> l.getObjectSummaries().stream())
                        .map(o -> Tuple.tuple(o.getKey(), o.getSize()))
                        .collect(Collectors.toList());
            } catch (final AmazonClientException e) {
                throw new IOException("Couldn't get list of files for path " + path, e);
            }

            deleteFiles(listOfFiles.stream()
                            .map(Tuple::v1)
                            .collect(Collectors.toList()),
                    true);

            final var removedBytes = listOfFiles.stream().map(Tuple::v2).reduce(0L, Long::sum);
            return Tuple.tuple(listOfFiles.size(), removedBytes);
        }

        @Override
        public void deleteFiles(final List<String> blobNames,
                                final boolean ignoreIfNotExists) throws IOException {

            final var partitionList = new ArrayList<List<String>>() {

                static final int MAX_DELETE_BULK = 10_000;

                @Override
                public List<String> get(final int index) {
                    final var start = index * MAX_DELETE_BULK;
                    final var end = Math.min(start + MAX_DELETE_BULK, blobNames.size());
                    return blobNames.subList(start, end);
                }

                @Override
                public int size() {
                    return (int) Math.ceil((double) blobNames.size() / (double) MAX_DELETE_BULK);
                }
            };

            for (var idx = 0; idx < partitionList.size(); idx++) {
                final var chunk = partitionList.get(idx);
                final var deleteObjectsRequest =
                        new DeleteObjectsRequest(bucketName)
                                .withKeys(chunk.toArray(new String[0]));
                try {
                    Permissions.doPrivileged(() -> client.deleteObjects(deleteObjectsRequest));
                } catch (final MultiObjectDeleteException e) {
                    for (final var err : e.getErrors()) {
                        logger.warn("Couldn't delete object: {}. Reason: [{}] {}",
                                err.getKey(), err.getCode(), err.getMessage());
                    }
                } catch (final AmazonClientException e) {
                    throw new IOException("Couldn't delete objects: " + blobNames, e);
                }
            }
        }

        @Override
        public List<String> listDirectories(final String path) throws IOException {
            try {
                return Permissions.doPrivileged(() -> list(listOfObjectsRequest(path)))
                        .stream()
                        .flatMap(listing -> listing.getCommonPrefixes().stream())
                        .map(prefix -> prefix.substring(path.length()))
                        .filter(Predicate.not(String::isEmpty))
                        .map(prefix -> prefix.substring(0, prefix.length() - 1))
                        .collect(Collectors.toList());
            } catch (final AmazonClientException e) {
                throw new IOException("Couldn't get list of directories for path " + path, e);
            }
        }

        @Override
        public Map<String, Long> listFiles(final String path, final String prefix) throws IOException {
            try {
                final var fullPath = path + prefix;
                return Permissions.doPrivileged(() -> list(listOfObjectsRequest(fullPath)))
                        .stream()
                        .flatMap(l -> l.getObjectSummaries().stream())
                        .collect(Collectors.toMap(o -> o.getKey().substring(path.length()), S3ObjectSummary::getSize));
            } catch (final AmazonClientException e) {
                throw new IOException("Couldn't get list of files for path " + path + " and prefix " + prefix, e);
            }
        }

        private ListObjectsV2Request listOfObjectsRequest(final String path) {
            return new ListObjectsV2Request()
                    .withBucketName(bucketName)
                    .withPrefix(path)
                    .withDelimiter("/");
        }

        private List<ListObjectsV2Result> list(final ListObjectsV2Request listObjectRequest) {
            final var objectSummariesList = new ArrayList<ListObjectsV2Result>();
            ListObjectsV2Result listing;
            do {
                listing = client.listObjectsV2(listObjectRequest);
                objectSummariesList.add(listing);
                final var nextToken = listing.getNextContinuationToken();
                listObjectRequest.setContinuationToken(nextToken);
            } while (listing.isTruncated());
            return objectSummariesList;
        }


    }

}
