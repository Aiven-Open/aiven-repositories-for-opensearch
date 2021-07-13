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

import java.io.IOException;
import java.net.Authenticator;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.util.Map;

import io.aiven.elasticsearch.repositories.Permissions;
import io.aiven.elasticsearch.repositories.RepositorySettingsProvider;
import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider;
import io.aiven.elasticsearch.repositories.security.EncryptionKeyProvider;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpTransportFactory;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Strings;
import com.google.common.net.HttpHeaders;
import org.opensearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcsSettingsProvider extends RepositorySettingsProvider<Storage> {

    static final String HTTP_USER_AGENT = "Aiven GCS Repository";

    @Override
    protected RepositoryStorageIOProvider<Storage> createRepositoryStorageIOProvider(final Settings settings)
            throws IOException {
        return Permissions.doPrivileged(() -> {
            final var gcsStorageSettings = GcsStorageSettings.create(settings);
            final var encryptionKeyProvider =
                    EncryptionKeyProvider.of(
                            gcsStorageSettings.publicKey(),
                            gcsStorageSettings.privateKey()
                    );
            final var client = createGcsClient(gcsStorageSettings);
            return new GcsRepositoryStorageIOProvider(client, encryptionKeyProvider);
        });
    }

    private Storage createGcsClient(final GcsStorageSettings gcsStorageSettings) {
        final StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder();
        if (!Strings.isNullOrEmpty(gcsStorageSettings.projectId())) {
            storageOptionsBuilder.setProjectId(gcsStorageSettings.projectId());
        }
        storageOptionsBuilder
                .setTransportOptions(
                        HttpTransportOptions.newBuilder()
                                .setHttpTransportFactory(createHttpTransportFactory(gcsStorageSettings))
                                .setConnectTimeout(gcsStorageSettings.connectionTimeout())
                                .setReadTimeout(gcsStorageSettings.readTimeout())
                                .build())
                .setHeaderProvider(() -> Map.of(HttpHeaders.USER_AGENT, HTTP_USER_AGENT))
                .setCredentials(gcsStorageSettings.gcsCredentials());

        return storageOptionsBuilder.build().getService();
    }

    private HttpTransportFactory createHttpTransportFactory(final GcsStorageSettings gcsStorageSettings) {
        if (!Strings.isNullOrEmpty(gcsStorageSettings.getProxyHost())) {
            return new ProxyHttpTransportFactory(
                    gcsStorageSettings.getProxyHost(),
                    gcsStorageSettings.getProxyPort(),
                    gcsStorageSettings.getProxyUsername(),
                    gcsStorageSettings.getProxyUserPassword()
            );
        }
        return new HttpTransportOptions.DefaultHttpTransportFactory();
    }

    private static final class ProxyHttpTransportFactory extends HttpTransportOptions.DefaultHttpTransportFactory {

        private static final Logger LOGGER = LoggerFactory.getLogger(GcsSettingsProvider.class);

        private final String proxyHost;

        private final int proxyPort;

        private final String proxyUsername;

        private final char[] proxyUserPassword;

        public ProxyHttpTransportFactory(final String proxyHost,
                                         final int proxyPort,
                                         final String proxyUsername,
                                         final char[] proxyUserPassword) {
            this.proxyHost = proxyHost;
            this.proxyPort = proxyPort;
            this.proxyUsername = proxyUsername;
            this.proxyUserPassword = proxyUserPassword;
        }

        @Override
        public HttpTransport create() {
            LOGGER.info("Create HttpTransportFactory with proxy support. Host: {} and port: {}", proxyHost, proxyPort);
            if (!Strings.isNullOrEmpty(proxyUsername)
                    && !Strings.isNullOrEmpty(String.valueOf(proxyUserPassword))) {
                LOGGER.info("Set user/pwd Authenticator: user is {} and pwd {}",
                        proxyUsername,
                        proxyUserPassword.length == 0 ? "is empty" : "is not empty");
                Authenticator.setDefault(new Authenticator() {
                    @Override
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(proxyUsername, proxyUserPassword);
                    }
                });
            }
            return new NetHttpTransport.Builder()
                    .setProxy(new Proxy(Proxy.Type.SOCKS,
                            new InetSocketAddress(proxyHost, proxyPort))).build();
        }
    }

}
