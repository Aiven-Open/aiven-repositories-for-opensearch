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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AzureClientTest {

    BlobServiceClient blobServiceClient;

    @Mock
    ExecutorService executorService;

    @BeforeEach
    void setUp() {
        blobServiceClient =
                new BlobServiceClientBuilder()
                        .connectionString(
                                String.format(
                                        AzureStorageSettings.AZURE_CONNECTION_STRING_TEMPLATE,
                                        "some_acc_name", "some_acc_key")
                        ).buildClient();
    }

    @Test
    void shutdownHttpPoolOnInterruptedException() throws Exception {
        final var azureClient = new AzureClient(blobServiceClient, executorService);
        when(executorService.awaitTermination(AzureClient.HTTP_POOL_AWAIT_TERMINATION, TimeUnit.MILLISECONDS))
                .thenThrow(InterruptedException.class);

        azureClient.close();

        verify(executorService).shutdown();
        verify(executorService).shutdownNow();
    }

    @Test
    void shutdownHttpPoolAfterMaxAttempts() throws Exception {
        final var azureClient = new AzureClient(blobServiceClient, executorService);
        when(executorService
                .awaitTermination(AzureClient.HTTP_POOL_AWAIT_TERMINATION, TimeUnit.MILLISECONDS)
        ).thenReturn(false);
        when(executorService.isShutdown()).thenReturn(false);

        azureClient.close();

        verify(executorService, times(AzureClient.MAX_HTTP_POOL_SHUTDOWN_ATTEMPTS + 1)).isShutdown();
        verify(executorService).shutdown();
        verify(executorService).shutdownNow();
    }

}
