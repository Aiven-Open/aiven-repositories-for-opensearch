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

package io.aiven.elasticsearch.repositories.gcs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;

import io.aiven.elasticsearch.repositories.DummySecureSettings;
import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider.StorageIO;
import io.aiven.elasticsearch.repositories.io.CryptoIOProvider;

import com.google.cloud.storage.Storage;

final class GcsBucket {

    static final String MARKER = "jmh-benchmark-data";

    private static final DateTimeFormatter TRIAL_TIMESTAMP = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    private final Storage storage;

    private final String bucketName;

    private final String basePath;

    private GcsBucket(final Storage storage, final String bucketName, final String basePath) {
        this.storage = storage;
        this.bucketName = bucketName;
        this.basePath = basePath;
    }

    static GcsBucket fromSystemProperties() throws IOException {
        final var bucketName = System.getProperty("bench.gcs.bucket", "");
        if (bucketName.isEmpty()) {
            throw new IllegalStateException("Set the bucket with -PgcsBucketName=<bucket>");
        }
        final var basePath = System.getProperty("bench.gcs.base_path", "jmh").replaceAll("^/+|/+$", "");
        if (basePath.isEmpty()) {
            throw new IllegalStateException("gcsBasePath must not be empty");
        }
        final var clientName = GcsClientSettings.DEFAULT_CLIENT_NAME;
        // GcsClientSettings requires the RSA key settings, the storage client doesn't use them.
        final var secureSettings = new DummySecureSettings()
            .setFile(GcsClientSettings.CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace(clientName).getKey(), credentials())
            .setFile(GcsClientSettings.PUBLIC_KEY_FILE.getConcreteSettingForNamespace(clientName).getKey(), new byte[0])
            .setFile(GcsClientSettings.PRIVATE_KEY_FILE.getConcreteSettingForNamespace(clientName).getKey(), new byte[0]);
        final var clientSettings = GcsClientSettings.create(Settings.builder().setSecureSettings(secureSettings).build())
            .get(clientName);
        return new GcsBucket(new GcsClientProvider().buildClient(clientSettings, Settings.EMPTY), bucketName, basePath);
    }

    StorageIO storageIO(final CryptoIOProvider cryptoIOProvider) {
        return new GcsRepositoryStorageIOProvider.GcsStorageIO(storage, bucketName, cryptoIOProvider);
    }

    void claimBasePath() throws IOException {
        final var stored = storageIO(CryptoIOProvider.UNENCRYPTED);
        if (stored.exists(markerPath())) {
            return;
        }
        if (storage.list(bucketName, Storage.BlobListOption.prefix(basePath + "/"), Storage.BlobListOption.pageSize(1))
            .getValues().iterator().hasNext()) {
            throw new IllegalStateException(
                location(basePath) + " has data but no " + MARKER + " object. Choose an empty gcsBasePath.");
        }
        stored.write(markerPath(), new ByteArrayInputStream(new byte[0]), 0, false);
    }

    String newTrialPath() {
        return basePath + "/" + LocalDateTime.now().format(TRIAL_TIMESTAMP) + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    void deleteTrialPath(final String trialPath) {
        try {
            storageIO(CryptoIOProvider.UNENCRYPTED).deleteDirectories(trialPath + "/");
        } catch (final IOException | RuntimeException e) {
            System.err.println("Failed to delete " + location(trialPath) + ", run jmhCleanGcs: " + e);
        }
    }

    Tuple<Integer, Long> deleteBasePath() throws IOException {
        final var stored = storageIO(CryptoIOProvider.UNENCRYPTED);
        if (!stored.exists(markerPath())) {
            throw new IllegalStateException("No " + MARKER + " object in " + location(basePath) + ", nothing deleted");
        }
        return stored.deleteDirectories(basePath + "/");
    }

    String location(final String path) {
        return "gs://" + bucketName + "/" + path + "/";
    }

    String basePath() {
        return basePath;
    }

    private String markerPath() {
        return basePath + "/" + MARKER;
    }

    private static byte[] credentials() throws IOException {
        final var configured = System.getProperty("bench.gcs.credentials", "");
        final var path = configured.isEmpty()
            ? Path.of(System.getProperty("user.home"), ".config/gcloud/application_default_credentials.json")
            : Path.of(configured);
        if (!Files.exists(path)) {
            throw new IllegalStateException(
                path + " doesn't exist. Set -PgcsCredentialsPath, GOOGLE_APPLICATION_CREDENTIALS"
                    + " or run gcloud auth application-default login");
        }
        return Files.readAllBytes(path);
    }
}
