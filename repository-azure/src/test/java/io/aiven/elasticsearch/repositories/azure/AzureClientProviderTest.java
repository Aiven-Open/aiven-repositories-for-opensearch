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

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import io.aiven.elasticsearch.repositories.DummySecureSettings;
import io.aiven.elasticsearch.repositories.RsaKeyAwareTest;

import com.azure.core.http.HttpPipeline;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.http.policy.UserAgentPolicy;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RequestRetryPolicy;
import org.elasticsearch.common.settings.Settings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.commons.support.HierarchyTraversalMode;
import org.junit.platform.commons.support.ReflectionSupport;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static io.aiven.elasticsearch.repositories.CommonSettings.RepositorySettings.MAX_RETRIES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AzureClientProviderTest extends RsaKeyAwareTest {

    @Mock
    ExecutorService executorService;

    @Test
    void shutdownHttpPoolOnInterruptedException() throws Exception {
        final var azureClientProvider = new AzureClientProvider();
        azureClientProvider.httpPoolExecutorService = executorService;
        when(executorService.awaitTermination(AzureClientProvider.HTTP_POOL_AWAIT_TERMINATION, TimeUnit.MILLISECONDS))
                .thenThrow(InterruptedException.class);

        azureClientProvider.close();

        verify(executorService).shutdown();
        verify(executorService).shutdownNow();
    }

    @Test
    void shutdownHttpPoolWithoutMaxAttempts() throws Exception {
        final var azureClientProvider = new AzureClientProvider();
        azureClientProvider.httpPoolExecutorService = executorService;
        when(executorService.awaitTermination(AzureClientProvider.HTTP_POOL_AWAIT_TERMINATION, TimeUnit.MILLISECONDS))
                .thenReturn(true);
        when(executorService.isShutdown()).thenReturn(true);

        azureClientProvider.close();

        verify(executorService).shutdown();
        verify(executorService).awaitTermination(
                AzureClientProvider.HTTP_POOL_AWAIT_TERMINATION, TimeUnit.MILLISECONDS);
        verify(executorService, never()).shutdownNow();
    }

    @Test
    void shutdownHttpPoolAfterMaxAttempts() throws Exception {
        final var azureClientProvider = new AzureClientProvider();
        azureClientProvider.httpPoolExecutorService = executorService;
        when(executorService
                .awaitTermination(AzureClientProvider.HTTP_POOL_AWAIT_TERMINATION, TimeUnit.MILLISECONDS)
        ).thenReturn(false);
        when(executorService.isShutdown()).thenReturn(false);

        azureClientProvider.close();

        verify(executorService, times(AzureClientProvider.MAX_HTTP_POOL_SHUTDOWN_ATTEMPTS))
                .awaitTermination(AzureClientProvider.HTTP_POOL_AWAIT_TERMINATION, TimeUnit.MILLISECONDS);
        verify(executorService, times(AzureClientProvider.MAX_HTTP_POOL_SHUTDOWN_ATTEMPTS + 1))
                .isShutdown();
        verify(executorService).shutdown();
        verify(executorService).shutdownNow();
    }

    @Test
    void testBuildClient() throws Exception {
        final var azureClientProvider = new AzureClientProvider();
        final var settings = createSettings();
        final var repoSettings =
                Settings.builder()
                        .put(AzureRepositoryStorageIOProvider.CONTAINER_NAME.getKey(), "some_container")
                        .put("some_settings_2", 210)
                        .build();

        final var client = azureClientProvider
                .buildClientIfNeeded(AzureClientSettings.create(settings), repoSettings);

        assertEquals("AZURE_ACCOUNT", client.getAccountName());

        final var requestRetryPolicy  = assertPolicy(client.getHttpPipeline(), RequestRetryPolicy.class);
        assertPolicy(client.getHttpPipeline(), UserAgentPolicy.class);
        final var requestRetryOptions = extractRequestRetryOptions(requestRetryPolicy);
        assertEquals(3, requestRetryOptions.getMaxTries());
    }

    @Test
    void testMaxRetriesOverridesClientSettings() throws Exception {
        final var azureClientProvider = new AzureClientProvider();
        final var settings = createSettings();
        final var repoSettings =
                Settings.builder()
                        .put(AzureRepositoryStorageIOProvider.CONTAINER_NAME.getKey(), "some_container")
                        .put(MAX_RETRIES.getKey(), 20)
                        .put("some_settings_2", 210)
                        .build();

        final var client = azureClientProvider
                .buildClientIfNeeded(AzureClientSettings.create(settings), repoSettings);

        assertEquals("AZURE_ACCOUNT", client.getAccountName());

        final var requestRetryPolicy  = assertPolicy(client.getHttpPipeline(), RequestRetryPolicy.class);
        assertPolicy(client.getHttpPipeline(), UserAgentPolicy.class);
        final var requestRetryOptions = extractRequestRetryOptions(requestRetryPolicy);
        assertEquals(20, requestRetryOptions.getMaxTries());
    }

    private Settings createSettings() throws IOException  {
        final var secureSettings =
                new DummySecureSettings()
                        .setString(AzureClientSettings.AZURE_ACCOUNT.getKey(), "AZURE_ACCOUNT")
                        .setString(AzureClientSettings.AZURE_ACCOUNT_KEY.getKey(), "AZURE_ACCOUNT_KEY")
                        .setFile(AzureClientSettings.PUBLIC_KEY_FILE.getKey(), Files.newInputStream(publicKeyPem))
                        .setFile(AzureClientSettings.PRIVATE_KEY_FILE.getKey(), Files.newInputStream(privateKeyPem));

        return Settings.builder()
                .put(AzureClientSettings.MAX_RETRIES.getKey(), 12)
                .setSecureSettings(secureSettings)
                .build();
    }

    @SuppressWarnings("unchecked")
    private <T extends HttpPipelinePolicy> T assertPolicy(final HttpPipeline httpPipeline,
                                                          final Class<T> policyClazz) throws Exception {
        for (var i = 0; i < httpPipeline.getPolicyCount(); i++) {
            if (httpPipeline.getPolicy(i).getClass().equals(policyClazz)) {
                return (T) httpPipeline.getPolicy(i);
            }
        }
        throw new IllegalArgumentException("Couldn't find policy class " + policyClazz);
    }

    private RequestRetryOptions extractRequestRetryOptions(
            final RequestRetryPolicy requestRetryPolicy) throws Exception {
        final var field = ReflectionSupport.findFields(RequestRetryPolicy.class, f -> f
                        .getName().equals("requestRetryOptions"),
                HierarchyTraversalMode.TOP_DOWN).get(0);
        field.setAccessible(true);
        return (RequestRetryOptions) field.get(requestRetryPolicy);
    }
}
