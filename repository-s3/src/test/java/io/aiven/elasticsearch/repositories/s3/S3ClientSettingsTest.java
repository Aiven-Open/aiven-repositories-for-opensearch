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

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import io.aiven.elasticsearch.repositories.DummySecureSettings;
import io.aiven.elasticsearch.repositories.RsaKeyAwareTest;

import com.amazonaws.ClientConfiguration;
import org.junit.jupiter.api.Test;

import static io.aiven.elasticsearch.repositories.s3.S3ClientSettings.AWS_ACCESS_KEY_ID;
import static io.aiven.elasticsearch.repositories.s3.S3ClientSettings.AWS_SECRET_ACCESS_KEY;
import static io.aiven.elasticsearch.repositories.s3.S3ClientSettings.ENDPOINT;
import static io.aiven.elasticsearch.repositories.s3.S3ClientSettings.MAX_RETRIES;
import static io.aiven.elasticsearch.repositories.s3.S3ClientSettings.PRIVATE_KEY_FILE;
import static io.aiven.elasticsearch.repositories.s3.S3ClientSettings.PUBLIC_KEY_FILE;
import static io.aiven.elasticsearch.repositories.s3.S3ClientSettings.READ_TIMEOUT;
import static io.aiven.elasticsearch.repositories.s3.S3ClientSettings.USE_THROTTLE_RETRIES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class S3ClientSettingsTest extends RsaKeyAwareTest {

    @Test
    void failsForEmptyAwsEndpoint() throws IOException  {
        final var secureSettings =
                new DummySecureSettings()
                        .setString(AWS_ACCESS_KEY_ID.getConcreteSettingForNamespace("default").getKey(),
                                "AWS_ACCESS_KEY_ID")
                        .setString(AWS_SECRET_ACCESS_KEY.getConcreteSettingForNamespace("default").getKey(),
                                "AWS_SECRET_ACCESS_KEY")
                        .setFile(PUBLIC_KEY_FILE.getConcreteSettingForNamespace("default").getKey(),
                                Files.newInputStream(publicKeyPem))
                        .setFile(PRIVATE_KEY_FILE.getConcreteSettingForNamespace("default").getKey(),
                                Files.newInputStream(privateKeyPem));

        final var noEndpointSettings = Settings.builder().setSecureSettings(secureSettings).build();

        final var t =
                assertThrows(IllegalArgumentException.class, () -> S3ClientSettings.create(noEndpointSettings));

        assertEquals("Settings with name aiven.s3.default.client.endpoint hasn't been set", t.getMessage());
    }

    @Test
    void failsForEmptyAwsAccessKeyID() throws IOException {
        final var secureSettings =
                new DummySecureSettings()
                        .setString(AWS_SECRET_ACCESS_KEY.getConcreteSettingForNamespace("default").getKey(),
                                "AWS_SECRET_ACCESS_KEY")
                        .setString(ENDPOINT.getConcreteSettingForNamespace("default").getKey(),
                                "ENDPOINT")
                        .setFile(PUBLIC_KEY_FILE.getConcreteSettingForNamespace("default").getKey(),
                                Files.newInputStream(publicKeyPem))
                        .setFile(PRIVATE_KEY_FILE.getConcreteSettingForNamespace("default").getKey(),
                                Files.newInputStream(privateKeyPem));

        final var noAwsAccessKeyId = Settings.builder().setSecureSettings(secureSettings).build();

        final var t =
                assertThrows(IllegalArgumentException.class, () -> S3ClientSettings.create(noAwsAccessKeyId));

        assertEquals(
                "Settings with name aiven.s3.default.client.aws_access_key_id hasn't been set",
                t.getMessage());
    }

    @Test
    void failsForEmptyAwsSecretAccessKey() throws IOException {
        final var secureSettings =
                new DummySecureSettings()
                        .setString(AWS_ACCESS_KEY_ID.getConcreteSettingForNamespace("default").getKey(),
                                "AWS_ACCESS_KEY_ID")
                        .setString(ENDPOINT.getConcreteSettingForNamespace("default").getKey(), "ENDPOINT")
                        .setFile(PUBLIC_KEY_FILE.getConcreteSettingForNamespace("default").getKey(),
                                Files.newInputStream(publicKeyPem))
                        .setFile(PRIVATE_KEY_FILE.getConcreteSettingForNamespace("default").getKey(),
                                Files.newInputStream(privateKeyPem));

        final var noAwsAccessKeyId = Settings.builder().setSecureSettings(secureSettings).build();

        final var t =
                assertThrows(IllegalArgumentException.class, () -> S3ClientSettings.create(noAwsAccessKeyId));

        assertEquals(
                "Settings with name aiven.s3.default.client.aws_secret_access_key hasn't been set",
                t.getMessage());
    }

    @Test
    void failsForEmptyPublicKey() throws IOException {
        final var settingsBuilder = Settings.builder().put(S3ClientSettings.ENDPOINT.getKey(), "endpoint");
        final var secureSettings =
                new DummySecureSettings()
                        .setString(AWS_SECRET_ACCESS_KEY.getConcreteSettingForNamespace("default").getKey(),
                                "AWS_SECRET_ACCESS_KEY")
                        .setString(AWS_ACCESS_KEY_ID.getConcreteSettingForNamespace("default").getKey(),
                                "AWS_ACCESS_KEY_ID")
                        .setString(ENDPOINT.getConcreteSettingForNamespace("default").getKey(),
                                "ENDPOINT")
                        .setFile(PRIVATE_KEY_FILE.getConcreteSettingForNamespace("default").getKey(),
                                Files.newInputStream(privateKeyPem));
        final var t =
                assertThrows(IllegalArgumentException.class, () ->
                        S3ClientSettings.create(settingsBuilder.setSecureSettings(secureSettings).build()));

        assertEquals("Settings with name aiven.s3.default.public_key_file hasn't been set", t.getMessage());
    }

    @Test
    void failsForEmptyPrivateKey() throws IOException {
        final var settingsBuilder = Settings.builder().put(S3ClientSettings.ENDPOINT.getKey(), "endpoint");
        final var secureSettings =
                new DummySecureSettings()
                        .setString(AWS_SECRET_ACCESS_KEY.getConcreteSettingForNamespace("default").getKey(),
                                "AWS_SECRET_ACCESS_KEY")
                        .setString(AWS_ACCESS_KEY_ID.getConcreteSettingForNamespace("default").getKey(),
                                "AWS_ACCESS_KEY_ID")
                        .setString(ENDPOINT.getConcreteSettingForNamespace("default").getKey(),
                                "ENDPOINT")
                        .setFile(PUBLIC_KEY_FILE.getConcreteSettingForNamespace("default").getKey(),
                                Files.newInputStream(publicKeyPem));
        final var t =
                assertThrows(IllegalArgumentException.class, () ->
                        S3ClientSettings.create(settingsBuilder.setSecureSettings(secureSettings).build()));

        assertEquals("Settings with name aiven.s3.default.private_key_file hasn't been set", t.getMessage());
    }

    @Test
    void loadDefaultSettings() throws IOException {
        final var secureSettings =
                new DummySecureSettings()
                        .setString(AWS_ACCESS_KEY_ID.getConcreteSettingForNamespace("default").getKey(),
                                "AWS_ACCESS_KEY_ID")
                        .setString(AWS_SECRET_ACCESS_KEY.getConcreteSettingForNamespace("default").getKey(),
                                "AWS_SECRET_ACCESS_KEY")
                        .setString(ENDPOINT.getConcreteSettingForNamespace("default").getKey(),
                                "endpoint")
                        .setFile(PUBLIC_KEY_FILE.getConcreteSettingForNamespace("default").getKey(),
                                Files.newInputStream(publicKeyPem))
                        .setFile(PRIVATE_KEY_FILE.getConcreteSettingForNamespace("default").getKey(),
                                Files.newInputStream(privateKeyPem));

        final var settings =
                Settings.builder().setSecureSettings(secureSettings).build();

        final var s3ClientSettings = S3ClientSettings.create(settings).get("default");

        assertEquals(s3ClientSettings.awsCredentials().getAWSAccessKeyId(), "AWS_ACCESS_KEY_ID");
        assertEquals(s3ClientSettings.awsCredentials().getAWSSecretKey(), "AWS_SECRET_ACCESS_KEY");
        assertEquals("endpoint", s3ClientSettings.endpoint());
        assertEquals(ClientConfiguration.DEFAULT_RETRY_POLICY.getMaxErrorRetry(), s3ClientSettings.maxRetries());
        assertEquals(ClientConfiguration.DEFAULT_THROTTLE_RETRIES, s3ClientSettings.useThrottleRetries());
        assertEquals(ClientConfiguration.DEFAULT_SOCKET_TIMEOUT, s3ClientSettings.readTimeout());
    }

    @Test
    void overrideDefaultSettings() throws IOException {
        final var secureSettings =
                new DummySecureSettings()
                        .setString(AWS_ACCESS_KEY_ID.getConcreteSettingForNamespace("default").getKey(),
                                "AWS_ACCESS_KEY_ID")
                        .setString(AWS_SECRET_ACCESS_KEY.getConcreteSettingForNamespace("default").getKey(),
                                "AWS_SECRET_ACCESS_KEY")
                        .setString(ENDPOINT.getConcreteSettingForNamespace("default").getKey(),
                                "http://endpoint")
                        .setFile(PUBLIC_KEY_FILE.getConcreteSettingForNamespace("default").getKey(),
                                Files.newInputStream(publicKeyPem))
                        .setFile(PRIVATE_KEY_FILE.getConcreteSettingForNamespace("default").getKey(),
                                Files.newInputStream(privateKeyPem));

        final var settings =
                Settings.builder()
                        .put(MAX_RETRIES.getConcreteSettingForNamespace("default").getKey(), 12)
                        .put(READ_TIMEOUT.getConcreteSettingForNamespace("default").getKey(),
                                TimeValue.timeValueMillis(1000L))
                        .put(USE_THROTTLE_RETRIES.getConcreteSettingForNamespace("default").getKey(), false)
                        .setSecureSettings(secureSettings)
                        .build();

        final var s3ClientSettings = S3ClientSettings.create(settings).get("default");

        assertEquals(s3ClientSettings.awsCredentials().getAWSAccessKeyId(), "AWS_ACCESS_KEY_ID");
        assertEquals(s3ClientSettings.awsCredentials().getAWSSecretKey(), "AWS_SECRET_ACCESS_KEY");
        assertEquals("http://endpoint", s3ClientSettings.endpoint());
        assertEquals(12, s3ClientSettings.maxRetries());
        assertFalse(s3ClientSettings.useThrottleRetries());
        assertEquals(1000L, s3ClientSettings.readTimeout());
    }

}
