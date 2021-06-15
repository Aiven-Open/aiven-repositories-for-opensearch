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

package io.aiven.elasticsearch.repositories.azure;

import java.net.Authenticator;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.azure.core.http.ProxyOptions;
import com.azure.core.http.okhttp.OkHttpAsyncHttpClientBuilder;
import com.azure.core.http.policy.UserAgentPolicy;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import okhttp3.Dispatcher;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AzureClient implements AutoCloseable {

    private final Logger logger = LoggerFactory.getLogger(AzureClient.class);

    static final String HTTP_USER_AGENT = "aiven-azure-repository";

    static final long HTTP_POOL_AWAIT_TERMINATION = 800L;

    static final int MAX_HTTP_POOL_SHUTDOWN_ATTEMPTS = 3;

    private final BlobServiceClient blobServiceClient;

    /**
     * Thread pool for Azure SDK HTTP client, must be closed explicitly
     */
    private final ExecutorService httpPoolExecutorService;

    protected AzureClient(final BlobServiceClient blobServiceClient, final ExecutorService httpPoolExecutorService) {
        this.blobServiceClient = blobServiceClient;
        this.httpPoolExecutorService = httpPoolExecutorService;
    }

    public BlobServiceClient blobServiceClient() {
        return blobServiceClient;
    }

    public static AzureClient create(final AzureStorageSettings azureStorageSettings) {
        final var httpThreadPoolSettings = azureStorageSettings.httpThreadPoolSettings();
        final var httpClientExecutor = new ThreadPoolExecutor(
                httpThreadPoolSettings.minThreads(), httpThreadPoolSettings.maxThreads(),
                httpThreadPoolSettings.keepAlive(), TimeUnit.SECONDS,
                new SynchronousQueue<>()
        );
        final var okHttpAsyncHttpClientBuilder =
                new OkHttpAsyncHttpClientBuilder()
                        .dispatcher(new Dispatcher(httpClientExecutor));
        if (!Strings.isNullOrEmpty(azureStorageSettings.proxyHost())) {
            okHttpAsyncHttpClientBuilder.proxy(
                    new ProxyOptions(
                            ProxyOptions.Type.SOCKS5,
                            new InetSocketAddress(azureStorageSettings.proxyHost(), azureStorageSettings.proxyPort())
                    )
            );
            if (!Strings.isNullOrEmpty(azureStorageSettings.proxyUsername())
                    && !Strings.isNullOrEmpty(String.valueOf(azureStorageSettings.proxyUserPassword()))) {
                Authenticator.setDefault(new Authenticator() {
                    @Override
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(
                                azureStorageSettings.proxyUsername(),
                                azureStorageSettings.proxyUserPassword()
                        );
                    }
                });
            }
        }
        final var blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(azureStorageSettings.azureConnectionString())
                .retryOptions(
                        new RequestRetryOptions(
                                RetryPolicyType.EXPONENTIAL, azureStorageSettings.maxRetries(),
                                null, null, null, null))
                .addPolicy(new UserAgentPolicy(HTTP_USER_AGENT))
                .httpClient(okHttpAsyncHttpClientBuilder.build())
                .buildClient();
        return new AzureClient(blobServiceClient, httpClientExecutor);
    }

    @Override
    public void close() {
        var shutdownAttempts = 0;
        try {
            httpPoolExecutorService.shutdown(); //disable new tasks from been submitted
            do {
                if (httpPoolExecutorService.awaitTermination(HTTP_POOL_AWAIT_TERMINATION, TimeUnit.MILLISECONDS)) {
                    break;
                }
                if (!httpPoolExecutorService.isShutdown()) {
                    TimeUnit.MILLISECONDS.sleep(HTTP_POOL_AWAIT_TERMINATION);
                    shutdownAttempts++;
                }
            } while (shutdownAttempts != MAX_HTTP_POOL_SHUTDOWN_ATTEMPTS);
            if (!httpPoolExecutorService.isShutdown()) {
                logger.warn("After {} attempts HTTP pull hasn't been shut down. Shut down it forcefully",
                        MAX_HTTP_POOL_SHUTDOWN_ATTEMPTS);
                httpPoolExecutorService.shutdownNow();
            }
        } catch (final InterruptedException e) {
            logger.warn("Got InterruptedException. Shutdown pull", e);
            httpPoolExecutorService.shutdownNow();
        }
    }




}
