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

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.opensearch.common.settings.Settings;

import io.aiven.elasticsearch.repositories.ClientProvider;

import com.azure.core.http.okhttp.OkHttpAsyncHttpClientBuilder;
import com.azure.core.http.policy.UserAgentPolicy;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import okhttp3.Dispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static io.aiven.elasticsearch.repositories.CommonSettings.RepositorySettings.MAX_RETRIES;

class AzureClientProvider extends ClientProvider<BlobServiceClient, AzureClientSettings> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureClientProvider.class);

    static final String HTTP_USER_AGENT = "aiven-azure-repository";

    static final long HTTP_POOL_AWAIT_TERMINATION = 800L;

    static final int MAX_HTTP_POOL_SHUTDOWN_ATTEMPTS = 3;

    /**
     * Thread pool for Azure SDK HTTP client, must be closed explicitly
     */
    protected volatile ExecutorService httpPoolExecutorService;

    @Override
    protected BlobServiceClient buildClient(final AzureClientSettings clientSettings,
                                            final Settings repositorySettings) {
        final var httpThreadPoolSettings = clientSettings.httpThreadPoolSettings();
        httpPoolExecutorService = new ThreadPoolExecutor(
                httpThreadPoolSettings.minThreads(), httpThreadPoolSettings.maxThreads(),
                httpThreadPoolSettings.keepAlive(), TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(httpThreadPoolSettings.workingQueueSize())
        );

        final var maxRetries = MAX_RETRIES.get(repositorySettings) > 0
                ? MAX_RETRIES.get(repositorySettings) : clientSettings.maxRetries();

        return new BlobServiceClientBuilder()
                .connectionString(clientSettings.azureConnectionString())
                .retryOptions(
                        new RequestRetryOptions(
                                RetryPolicyType.EXPONENTIAL, maxRetries,
                                null, null, null, null))
                .addPolicy(new UserAgentPolicy(HTTP_USER_AGENT))
                .httpClient(
                        new OkHttpAsyncHttpClientBuilder()
                                .dispatcher(new Dispatcher(httpPoolExecutorService))
                                .build())
                .buildClient();
    }

    @Override
    protected void closeClient() {
        if (Objects.nonNull(httpPoolExecutorService)) {
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
                    LOGGER.warn("After {} attempts HTTP pull hasn't been shut down. Shut down it forcefully",
                            MAX_HTTP_POOL_SHUTDOWN_ATTEMPTS);
                    httpPoolExecutorService.shutdownNow();
                }
            } catch (final InterruptedException e) {
                LOGGER.warn("Got InterruptedException. Shutdown pull", e);
                httpPoolExecutorService.shutdownNow();
            }
            httpPoolExecutorService = null;
        }
    }

}
