/*
 * Copyright 2021 Aiven Oy
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

import java.util.Objects;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import io.aiven.elasticsearch.repositories.ClientProvider;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import static io.aiven.elasticsearch.repositories.CommonSettings.RepositorySettings.MAX_RETRIES;

class S3ClientProvider extends ClientProvider<AmazonS3Client, S3ClientSettings> {

    static final String HTTP_USER_AGENT = "Aiven S3 Repository";

    static final Setting<String> ENDPOINT_NAME =
            Setting.simpleString(
                    "endpoint",
                    Setting.Property.NodeScope,
                    Setting.Property.Dynamic
            );

    @Override
    protected AmazonS3Client buildClient(final S3ClientSettings clientSettings,
                                   final Settings repositorySettings) {
        final var s3ClientBuilder = AmazonS3ClientBuilder.standard();

        final var maxRetries = MAX_RETRIES.exists(repositorySettings)
                ? MAX_RETRIES.get(repositorySettings)
                : clientSettings.maxRetries();
        final var endpoint = ENDPOINT_NAME.exists(repositorySettings)
                ? ENDPOINT_NAME.get(repositorySettings)
                : clientSettings.endpoint();

        final var clientConfiguration = new ClientConfiguration();
        clientConfiguration.setResponseMetadataCacheSize(0);
        clientConfiguration.setMaxErrorRetry(maxRetries);
        clientConfiguration.setUseThrottleRetries(clientSettings.useThrottleRetries());
        clientConfiguration.setSocketTimeout(clientSettings.readTimeout());
        clientConfiguration.setUserAgentPrefix(HTTP_USER_AGENT);

        s3ClientBuilder
                .withCredentials(new AWSStaticCredentialsProvider(clientSettings.awsCredentials()))
                .withClientConfiguration(clientConfiguration);
        s3ClientBuilder.withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(
                        endpoint, null));
        return (AmazonS3Client) s3ClientBuilder.build();
    }

    @Override
    protected void closeClient() {
        if (Objects.nonNull(client)) {
            client.shutdown();
        }
    }

}
