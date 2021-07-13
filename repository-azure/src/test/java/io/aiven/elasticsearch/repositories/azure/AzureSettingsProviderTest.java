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

import java.nio.file.Files;

import io.aiven.elasticsearch.repositories.DummySecureSettings;
import io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider;
import io.aiven.elasticsearch.repositories.RsaKeyAwareTest;

import com.azure.core.http.HttpPipeline;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.http.policy.UserAgentPolicy;
import com.azure.storage.common.policy.RequestRetryPolicy;
import org.opensearch.common.settings.Settings;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.support.HierarchyTraversalMode;
import org.junit.platform.commons.support.ReflectionSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AzureSettingsProviderTest extends RsaKeyAwareTest {

    @Test
    void providerInitialization() throws Exception {
        final var azureSettingsProvider = new AzureSettingsProvider();

        final var secureSettings =
                new DummySecureSettings()
                        .setString(AzureStorageSettings.AZURE_ACCOUNT.getKey(), "AZURE_ACCOUNT")
                        .setString(AzureStorageSettings.AZURE_ACCOUNT_KEY.getKey(), "AZURE_ACCOUNT_KEY")
                        .setFile(AzureStorageSettings.PUBLIC_KEY_FILE.getKey(), Files.newInputStream(publicKeyPem))
                        .setFile(AzureStorageSettings.PRIVATE_KEY_FILE.getKey(), Files.newInputStream(privateKeyPem));

        final var settings =
                Settings.builder()
                        .put(AzureStorageSettings.MAX_RETRIES.getKey(), 12)
                        .setSecureSettings(secureSettings)
                        .build();

        azureSettingsProvider.reload(AzureRepositoryPlugin.REPOSITORY_TYPE, settings);

        final var client = extractClient(azureSettingsProvider.repositoryStorageIOProvider());

        assertEquals("AZURE_ACCOUNT", client.blobServiceClient().getAccountName());

        assertPolicy(client.blobServiceClient().getHttpPipeline(), RequestRetryPolicy.class);
        assertPolicy(client.blobServiceClient().getHttpPipeline(), UserAgentPolicy.class);
    }

    private <T extends HttpPipelinePolicy> void assertPolicy(final HttpPipeline httpPipeline,
                                                             final Class<T> policyClazz) throws Exception {
        for (var i = 0; i < httpPipeline.getPolicyCount(); i++) {
            if (httpPipeline.getPolicy(i).getClass().equals(policyClazz)) {
                return;
            }
        }
        throw new IllegalArgumentException("Couldn't find policy class " + policyClazz);
    }

    private AzureClient extractClient(
            final RepositoryStorageIOProvider<AzureClient> storageIOProvider) throws Exception {
        final var field = ReflectionSupport.findFields(
                RepositoryStorageIOProvider.class, f ->
                        f.getName().equals("client"),
                HierarchyTraversalMode.TOP_DOWN).get(0);
        field.setAccessible(true);
        return (AzureClient) field.get(storageIOProvider);
    }


}
