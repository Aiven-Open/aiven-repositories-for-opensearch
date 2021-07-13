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

import java.nio.file.Files;

import io.aiven.elasticsearch.repositories.DummySecureSettings;
import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider;
import io.aiven.elasticsearch.repositories.RsaKeyAwareTest;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.support.HierarchyTraversalMode;
import org.junit.platform.commons.support.ReflectionSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3SettingsProviderTest extends RsaKeyAwareTest {

    @Test
    void throwsIllegalArgumentExceptionForEmptySettings() {
        final var t = assertThrows(IllegalArgumentException.class, () -> S3StorageSettings.create(Settings.EMPTY));
        assertEquals("Settings for AWS S3 haven't been set", t.getMessage());
    }

    @Test
    void providerInitialization() throws Exception {
        final var s3SettingsProvider = new S3SettingsProvider();

        final var secureSettings =
                new DummySecureSettings()
                        .setString(S3StorageSettings.AWS_ACCESS_KEY_ID.getKey(), "AWS_ACCESS_KEY_ID")
                        .setString(S3StorageSettings.AWS_SECRET_ACCESS_KEY.getKey(), "AWS_SECRET_ACCESS_KEY")
                        .setString(S3StorageSettings.ENDPOINT.getKey(), "http://endpoint")
                        .setFile(S3StorageSettings.PUBLIC_KEY_FILE.getKey(), Files.newInputStream(publicKeyPem))
                        .setFile(S3StorageSettings.PRIVATE_KEY_FILE.getKey(), Files.newInputStream(privateKeyPem));

        final var settings =
                Settings.builder()
                        .put(S3StorageSettings.MAX_RETRIES.getKey(), 12)
                        .put(S3StorageSettings.READ_TIMEOUT.getKey(), TimeValue.timeValueMillis(1000L))
                        .put(S3StorageSettings.USE_THROTTLE_RETRIES.getKey(), false)
                        .setSecureSettings(secureSettings)
                        .build();

        s3SettingsProvider.reload(S3RepositoryPlugin.REPOSITORY_TYPE, settings);

        final var client = extractClient(s3SettingsProvider.repositoryStorageIOProvider());
        final var amazonS3Client = (AmazonS3Client) client;

        assertEquals(S3SettingsProvider.HTTP_USER_AGENT, amazonS3Client.getClientConfiguration().getUserAgentPrefix());
        assertEquals(
                12,
                amazonS3Client.getClientConfiguration().getMaxErrorRetry()
        );
        assertFalse(amazonS3Client.getClientConfiguration().useThrottledRetries());
        assertEquals(1000L, amazonS3Client.getClientConfiguration().getSocketTimeout());
    }

    @Test
    void providerInitializationWithDefaultValues() throws Exception {
        final var s3SettingsProvider = new S3SettingsProvider();

        final var secureSettings =
                new DummySecureSettings()
                        .setString(S3StorageSettings.AWS_ACCESS_KEY_ID.getKey(), "AWS_ACCESS_KEY_ID")
                        .setString(S3StorageSettings.AWS_SECRET_ACCESS_KEY.getKey(), "AWS_SECRET_ACCESS_KEY")
                        .setString(S3StorageSettings.ENDPOINT.getKey(), "http://endpoint")
                        .setFile(S3StorageSettings.PUBLIC_KEY_FILE.getKey(), Files.newInputStream(publicKeyPem))
                        .setFile(S3StorageSettings.PRIVATE_KEY_FILE.getKey(), Files.newInputStream(privateKeyPem));

        final var settings =
                Settings.builder().setSecureSettings(secureSettings).build();

        s3SettingsProvider.reload(S3RepositoryPlugin.REPOSITORY_TYPE, settings);

        final var client = extractClient(s3SettingsProvider.repositoryStorageIOProvider());
        final var amazonS3Client = (AmazonS3Client) client;

        assertEquals(S3SettingsProvider.HTTP_USER_AGENT, amazonS3Client.getClientConfiguration().getUserAgentPrefix());
        assertEquals(
                ClientConfiguration.DEFAULT_RETRY_POLICY.getMaxErrorRetry(),
                amazonS3Client.getClientConfiguration().getMaxErrorRetry()
        );
        assertTrue(amazonS3Client.getClientConfiguration().useThrottledRetries());
        assertEquals(
                ClientConfiguration.DEFAULT_SOCKET_TIMEOUT,
                amazonS3Client.getClientConfiguration().getSocketTimeout()
        );
    }

    private AmazonS3 extractClient(final RepositoryStorageIOProvider<AmazonS3> storageIOProvider) throws Exception {
        final var field = ReflectionSupport.findFields(RepositoryStorageIOProvider.class, f -> f
                        .getName().equals("client"),
                HierarchyTraversalMode.TOP_DOWN).get(0);
        field.setAccessible(true);
        return (AmazonS3) field.get(storageIOProvider);
    }

}
