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
import java.net.Authenticator;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.net.Socket;
import java.security.SecureRandom;

import io.aiven.elasticsearch.repositories.Permissions;
import io.aiven.elasticsearch.repositories.RepositorySettingsProvider;
import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider;
import io.aiven.elasticsearch.repositories.security.EncryptionKeyProvider;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.http.SystemPropertyTlsKeyManagersProvider;
import com.amazonaws.http.conn.ssl.SdkTLSSocketFactory;
import com.amazonaws.internal.SdkSSLContext;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.protocol.HttpContext;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;

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
        if (!Strings.isNullOrEmpty(s3KeystoreSettings.proxyHost())) {
            final var sslCtx = SdkSSLContext.getPreferredSSLContext(
                    new SystemPropertyTlsKeyManagersProvider().getKeyManagers(),
                    new SecureRandom());
            clientConfiguration
                    .getApacheHttpClientConfig()
                    .setSslSocketFactory(
                            // this settings is open question. need tio verify that it works ok
                            new SdkTLSSocketFactory(sslCtx, SSLConnectionSocketFactory.STRICT_HOSTNAME_VERIFIER) {
                                @Override
                                public Socket createSocket(final HttpContext ctx) throws IOException {
                                    return new Socket(
                                            new Proxy(
                                                    Proxy.Type.SOCKS,
                                                    new InetSocketAddress(
                                                            s3KeystoreSettings.proxyHost(),
                                                            s3KeystoreSettings.proxyPort())));
                                }
                            });
            if (!Strings.isNullOrEmpty(s3KeystoreSettings.proxyUsername())
                    && !Strings.isNullOrEmpty(String.valueOf(s3KeystoreSettings.proxyUserPassword()))) {
                Authenticator.setDefault(new Authenticator() {
                    @Override
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(
                                s3KeystoreSettings.proxyUsername(),
                                s3KeystoreSettings.proxyUserPassword()
                        );
                    }
                });
            }
        }
        clientConfiguration.setResponseMetadataCacheSize(0);
        clientConfiguration.setMaxErrorRetry(s3KeystoreSettings.maxRetries());
        clientConfiguration.setUseThrottleRetries(s3KeystoreSettings.useThrottleRetries());
        clientConfiguration.setSocketTimeout(s3KeystoreSettings.readTimeout());
        clientConfiguration.setUserAgentPrefix(HTTP_USER_AGENT);

        s3ClientBuilder
                .withCredentials(new AWSStaticCredentialsProvider(s3KeystoreSettings.awsCredentials()))
                .withClientConfiguration(clientConfiguration)
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(
                                s3KeystoreSettings.endpoint(),
                                null));
        return s3ClientBuilder.build();
    }

}
