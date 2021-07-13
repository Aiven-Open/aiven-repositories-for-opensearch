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

import io.aiven.elasticsearch.repositories.Permissions;
import io.aiven.elasticsearch.repositories.RepositorySettingsProvider;
import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider;
import io.aiven.elasticsearch.repositories.security.EncryptionKeyProvider;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.opensearch.common.settings.Settings;

public class S3SettingsProvider extends RepositorySettingsProvider<AmazonS3> {

    static final String HTTP_USER_AGENT = "Aiven S3 Repository";

    @Override
    protected RepositoryStorageIOProvider<AmazonS3> createRepositoryStorageIOProvider(final Settings settings)
            throws IOException {
        return Permissions.doPrivileged(() -> {
            final var s3StorageSettings = S3StorageSettings.create(settings);
            final var encryptionKeyProvider =
                    EncryptionKeyProvider.of(s3StorageSettings.publicKey(), s3StorageSettings.privateKey());
            return new S3RepositoryStorageIOProvider(createClient(s3StorageSettings), encryptionKeyProvider);
        });
    }

    private AmazonS3 createClient(final S3StorageSettings s3KeystoreSettings) {
        final var s3ClientBuilder = AmazonS3ClientBuilder.standard();

        final var clientConfiguration = new ClientConfiguration();
        clientConfiguration.setResponseMetadataCacheSize(0);
        clientConfiguration.setMaxErrorRetry(s3KeystoreSettings.maxRetries());
        clientConfiguration.setUseThrottleRetries(s3KeystoreSettings.useThrottleRetries());
        clientConfiguration.setSocketTimeout(s3KeystoreSettings.readTimeout());
        clientConfiguration.setUserAgentPrefix(HTTP_USER_AGENT);

        s3ClientBuilder
                .withCredentials(new AWSStaticCredentialsProvider(s3KeystoreSettings.awsCredentials()))
                .withClientConfiguration(clientConfiguration);
        s3ClientBuilder.withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(
                        s3KeystoreSettings.endpoint(), null));
        return s3ClientBuilder.build();
    }

}
