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
import java.nio.file.Files;

import io.aiven.elasticsearch.repositories.DummySecureSettings;
import io.aiven.elasticsearch.repositories.RsaKeyAwareTest;

import com.amazonaws.ClientConfiguration;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class S3StorageSettingsTest extends RsaKeyAwareTest {

    @Test
    void failsForEmptyAwsEndpoint() throws IOException  {
        final var secureSettings =
                new DummySecureSettings()
                        .setString(S3StorageSettings.AWS_ACCESS_KEY_ID.getKey(), "AWS_ACCESS_KEY_ID")
                        .setString(S3StorageSettings.AWS_SECRET_ACCESS_KEY.getKey(), "AWS_SECRET_ACCESS_KEY")
                        .setFile(S3StorageSettings.PUBLIC_KEY_FILE.getKey(), Files.newInputStream(publicKeyPem))
                        .setFile(S3StorageSettings.PRIVATE_KEY_FILE.getKey(), Files.newInputStream(privateKeyPem));

        final var noEndpointSettings = Settings.builder().setSecureSettings(secureSettings).build();

        final var t =
                assertThrows(IllegalArgumentException.class, () -> S3StorageSettings.create(noEndpointSettings));

        assertEquals("Settings with name aiven.s3.client.endpoint hasn't been set", t.getMessage());
    }

    @Test
    void failsForEmptyAwsAccessKeyID() throws IOException {
        final var secureSettings =
                new DummySecureSettings()
                        .setString(S3StorageSettings.AWS_SECRET_ACCESS_KEY.getKey(), "AWS_SECRET_ACCESS_KEY")
                        .setString(S3StorageSettings.ENDPOINT.getKey(), "ENDPOINT")
                        .setFile(S3StorageSettings.PUBLIC_KEY_FILE.getKey(), Files.newInputStream(publicKeyPem))
                        .setFile(S3StorageSettings.PRIVATE_KEY_FILE.getKey(), Files.newInputStream(privateKeyPem));

        final var noAwsAccessKeyId = Settings.builder().setSecureSettings(secureSettings).build();

        final var t =
                assertThrows(IllegalArgumentException.class, () -> S3StorageSettings.create(noAwsAccessKeyId));

        assertEquals(
                "Settings with name aiven.s3.client.aws_access_key_id hasn't been set",
                t.getMessage());
    }

    @Test
    void failsForEmptyAwsSecretAccessKey() throws IOException {
        final var secureSettings =
                new DummySecureSettings()
                        .setString(S3StorageSettings.AWS_ACCESS_KEY_ID.getKey(), "AWS_ACCESS_KEY_ID")
                        .setString(S3StorageSettings.ENDPOINT.getKey(), "ENDPOINT")
                        .setFile(S3StorageSettings.PUBLIC_KEY_FILE.getKey(), Files.newInputStream(publicKeyPem))
                        .setFile(S3StorageSettings.PRIVATE_KEY_FILE.getKey(), Files.newInputStream(privateKeyPem));

        final var noAwsAccessKeyId = Settings.builder().setSecureSettings(secureSettings).build();

        final var t =
                assertThrows(IllegalArgumentException.class, () -> S3StorageSettings.create(noAwsAccessKeyId));

        assertEquals(
                "Settings with name aiven.s3.client.aws_secret_access_key hasn't been set",
                t.getMessage());
    }

    @Test
    void failsForEmptyPublicKey() throws IOException {
        final var settingsBuilder = Settings.builder().put(S3StorageSettings.ENDPOINT.getKey(), "endpoint");
        final var secureSettings =
                new DummySecureSettings()
                        .setString(S3StorageSettings.AWS_SECRET_ACCESS_KEY.getKey(), "AWS_SECRET_ACCESS_KEY")
                        .setString(S3StorageSettings.AWS_ACCESS_KEY_ID.getKey(), "AWS_ACCESS_KEY_ID")
                        .setString(S3StorageSettings.ENDPOINT.getKey(), "ENDPOINT")
                        .setFile(S3StorageSettings.PRIVATE_KEY_FILE.getKey(), Files.newInputStream(privateKeyPem));
        final var t =
                assertThrows(IllegalArgumentException.class, () ->
                        S3StorageSettings.create(settingsBuilder.setSecureSettings(secureSettings).build()));

        assertEquals("Settings with name aiven.s3.public_key_file hasn't been set", t.getMessage());
    }

    @Test
    void failsForEmptyPrivateKey() throws IOException {
        final var settingsBuilder = Settings.builder().put(S3StorageSettings.ENDPOINT.getKey(), "endpoint");
        final var secureSettings =
                new DummySecureSettings()
                        .setString(S3StorageSettings.AWS_SECRET_ACCESS_KEY.getKey(), "AWS_SECRET_ACCESS_KEY")
                        .setString(S3StorageSettings.AWS_ACCESS_KEY_ID.getKey(), "AWS_ACCESS_KEY_ID")
                        .setString(S3StorageSettings.ENDPOINT.getKey(), "ENDPOINT")
                        .setFile(S3StorageSettings.PUBLIC_KEY_FILE.getKey(), Files.newInputStream(publicKeyPem));
        final var t =
                assertThrows(IllegalArgumentException.class, () ->
                        S3StorageSettings.create(settingsBuilder.setSecureSettings(secureSettings).build()));

        assertEquals("Settings with name aiven.s3.private_key_file hasn't been set", t.getMessage());
    }

    @Test
    void loadDefaultSettings() throws IOException {
        final var secureSettings =
                new DummySecureSettings()
                        .setString(S3StorageSettings.AWS_ACCESS_KEY_ID.getKey(), "AWS_ACCESS_KEY_ID")
                        .setString(S3StorageSettings.AWS_SECRET_ACCESS_KEY.getKey(), "AWS_SECRET_ACCESS_KEY")
                        .setString(S3StorageSettings.ENDPOINT.getKey(), "endpoint")
                        .setFile(S3StorageSettings.PUBLIC_KEY_FILE.getKey(), Files.newInputStream(publicKeyPem))
                        .setFile(S3StorageSettings.PRIVATE_KEY_FILE.getKey(), Files.newInputStream(privateKeyPem));

        final var settings =
                Settings.builder().setSecureSettings(secureSettings).build();

        final var s3StorageSettings = S3StorageSettings.create(settings);

        assertEquals(s3StorageSettings.awsCredentials().getAWSAccessKeyId(), "AWS_ACCESS_KEY_ID");
        assertEquals(s3StorageSettings.awsCredentials().getAWSSecretKey(), "AWS_SECRET_ACCESS_KEY");
        assertEquals("endpoint", s3StorageSettings.endpoint());
        assertEquals(ClientConfiguration.DEFAULT_RETRY_POLICY.getMaxErrorRetry(), s3StorageSettings.maxRetries());
        assertEquals(ClientConfiguration.DEFAULT_THROTTLE_RETRIES, s3StorageSettings.useThrottleRetries());
        assertEquals(ClientConfiguration.DEFAULT_SOCKET_TIMEOUT, s3StorageSettings.readTimeout());
    }

    @Test
    void overrideDefaultSettings() throws IOException {
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

        final var s3StorageSettings = S3StorageSettings.create(settings);

        assertEquals(s3StorageSettings.awsCredentials().getAWSAccessKeyId(), "AWS_ACCESS_KEY_ID");
        assertEquals(s3StorageSettings.awsCredentials().getAWSSecretKey(), "AWS_SECRET_ACCESS_KEY");
        assertEquals("http://endpoint", s3StorageSettings.endpoint());
        assertEquals(12, s3StorageSettings.maxRetries());
        assertFalse(s3StorageSettings.useThrottleRetries());
        assertEquals(1000L, s3StorageSettings.readTimeout());
    }

}
