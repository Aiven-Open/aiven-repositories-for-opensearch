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
import java.net.URI;
import java.nio.file.Files;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import io.aiven.elasticsearch.repositories.CommonSettings;
import io.aiven.elasticsearch.repositories.DummySecureSettings;
import io.aiven.elasticsearch.repositories.RsaKeyAwareTest;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3Client;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.support.HierarchyTraversalMode;
import org.junit.platform.commons.support.ReflectionSupport;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3ClientProviderTest extends RsaKeyAwareTest {

    @Test
    void throwsIllegalArgumentExceptionForEmptySettings() {
        final var t = assertThrows(IllegalArgumentException.class, () -> S3ClientSettings.create(Settings.EMPTY));
        assertEquals("Settings for AWS S3 haven't been set", t.getMessage());
    }

    @Test
    void providerInitialization() throws Exception {
        final var s3ClientProvider = new S3ClientProvider();

        final var secureSettings =
                new DummySecureSettings()
                        .setString(
                                AWS_ACCESS_KEY_ID.getConcreteSettingForNamespace("default").getKey(),
                                "AWS_ACCESS_KEY_ID"
                        ).setString(
                                AWS_SECRET_ACCESS_KEY.getConcreteSettingForNamespace("default").getKey(),
                                "AWS_SECRET_ACCESS_KEY")
                        .setString(
                                ENDPOINT.getConcreteSettingForNamespace("default").getKey(),
                                "http://endpoint"
                        ).setFile(
                                PUBLIC_KEY_FILE.getConcreteSettingForNamespace("default").getKey(),
                                Files.newInputStream(publicKeyPem)
                        ).setFile(
                                PRIVATE_KEY_FILE.getConcreteSettingForNamespace("default").getKey(),
                                Files.newInputStream(privateKeyPem)
                        );

        final var settings =
                Settings.builder()
                        .put(MAX_RETRIES.getConcreteSettingForNamespace("default").getKey(), 12)
                        .put(READ_TIMEOUT.getConcreteSettingForNamespace("default").getKey(),
                                TimeValue.timeValueMillis(1000L))
                        .put(USE_THROTTLE_RETRIES.getConcreteSettingForNamespace("default").getKey(), false)
                        .setSecureSettings(secureSettings)
                        .build();
        final var repoSettings =
                Settings.builder()
                        .put("some_settings_1", 20)
                        .put("some_settings_2", 210)
                        .build();

        final var client =
                s3ClientProvider.buildClientIfNeeded(
                        S3ClientSettings.create(settings).get("default"),
                        repoSettings
                ).v2();
        final var amazonS3Client = (AmazonS3Client) client;

        assertEquals(S3ClientProvider.HTTP_USER_AGENT, amazonS3Client.getClientConfiguration().getUserAgentPrefix());
        assertEquals(
                12,
                amazonS3Client.getClientConfiguration().getMaxErrorRetry()
        );
        assertFalse(amazonS3Client.getClientConfiguration().useThrottledRetries());
        assertEquals(1000L, amazonS3Client.getClientConfiguration().getSocketTimeout());
    }

    @Test
    void providerInitializationWithDefaultValues() throws Exception {
        final var s3ClientProvider = new S3ClientProvider();
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
                Settings.builder().setSecureSettings(secureSettings).build();
        final var repoSettings =
                Settings.builder()
                        .put("some_settings_1", 20)
                        .put("some_settings_2", 210)
                        .build();


        final var client =
                s3ClientProvider.buildClientIfNeeded(
                        S3ClientSettings.create(settings).get("default"),
                        repoSettings
                ).v2();
        final var amazonS3Client = (AmazonS3Client) client;

        assertEquals(S3ClientProvider.HTTP_USER_AGENT, amazonS3Client.getClientConfiguration().getUserAgentPrefix());
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

    @Test
    void testMaxRetriesOverridesClientSettings() throws IOException {
        final var s3ClientProvider = new S3ClientProvider();
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
                Settings.builder().setSecureSettings(secureSettings).build();


        final var repoSettings =
                Settings.builder()
                        .put(CommonSettings.RepositorySettings.MAX_RETRIES.getKey(), 20)
                        .build();
        final var client =
                s3ClientProvider.buildClientIfNeeded(
                        S3ClientSettings.create(settings).get("default"),
                        repoSettings
                ).v2();
        final var amazonS3Client = (AmazonS3Client) client;

        assertEquals(S3ClientProvider.HTTP_USER_AGENT, amazonS3Client.getClientConfiguration().getUserAgentPrefix());
        assertEquals(
                20,
                amazonS3Client.getClientConfiguration().getMaxErrorRetry()
        );
        assertTrue(amazonS3Client.getClientConfiguration().useThrottledRetries());
        assertEquals(
                ClientConfiguration.DEFAULT_SOCKET_TIMEOUT,
                amazonS3Client.getClientConfiguration().getSocketTimeout()
        );
    }

    @Test
    void testEndpointOverridesClientSettings() throws Exception {
        final var s3ClientProvider = new S3ClientProvider();
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
                Settings.builder().setSecureSettings(secureSettings).build();


        final var repoSettings =
                Settings.builder()
                        .put(CommonSettings.RepositorySettings.MAX_RETRIES.getKey(), 20)
                        .put(S3ClientProvider.ENDPOINT_NAME.getKey(), "http://new-endpoint")
                        .build();
        final var client =
                s3ClientProvider.buildClientIfNeeded(
                        S3ClientSettings.create(settings).get("default"),
                        repoSettings
                ).v2();
        final var amazonS3Client = (AmazonS3Client) client;

        assertEquals(S3ClientProvider.HTTP_USER_AGENT, amazonS3Client.getClientConfiguration().getUserAgentPrefix());
        assertEquals(
                20,
                amazonS3Client.getClientConfiguration().getMaxErrorRetry()
        );
        assertTrue(amazonS3Client.getClientConfiguration().useThrottledRetries());
        assertEquals(new URI("http://new-endpoint"), extractEndpoint(amazonS3Client));
        assertEquals(
                ClientConfiguration.DEFAULT_SOCKET_TIMEOUT,
                amazonS3Client.getClientConfiguration().getSocketTimeout()
        );
    }

    private URI extractEndpoint(final AmazonS3Client amazonS3Client) throws Exception {
        final var field = ReflectionSupport.findFields(AmazonS3Client.class, f -> f
                        .getName().equals("endpoint"),
                HierarchyTraversalMode.TOP_DOWN).get(0);
        field.setAccessible(true);
        return (URI) field.get(amazonS3Client);
    }

}
