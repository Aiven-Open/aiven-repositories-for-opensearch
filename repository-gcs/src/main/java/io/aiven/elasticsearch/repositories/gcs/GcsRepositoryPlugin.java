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

import java.util.ArrayList;
import java.util.List;

import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import io.aiven.elasticsearch.repositories.AbstractRepositoryPlugin;

import com.google.cloud.storage.Storage;

public class GcsRepositoryPlugin extends AbstractRepositoryPlugin<Storage, GcsClientSettings>  {

    public static final String REPOSITORY_TYPE = "aiven-gcs";

    public GcsRepositoryPlugin(final Settings settings) {
        super(REPOSITORY_TYPE, settings, new GcsSettingsProvider());
    }

    @Override
    public List<Setting<?>> getSettings() {
        final List<Setting<?>> baseSettings = List.of(
                GcsClientSettings.CLIENT_NAME,
                GcsClientSettings.PRIVATE_KEY_FILE,
                GcsClientSettings.PUBLIC_KEY_FILE,
                GcsClientSettings.CREDENTIALS_FILE_SETTING,
                GcsClientSettings.PROJECT_ID,
                GcsClientSettings.CONNECTION_TIMEOUT,
                GcsClientSettings.READ_TIMEOUT,
                GcsClientSettings.PROXY_HOST,
                GcsClientSettings.PROXY_PORT,
                GcsClientSettings.PROXY_USER_NAME,
                GcsClientSettings.PROXY_USER_PASSWORD
        );
        
        // Add client-specific keystore settings for common client names
        // This prevents OpenSearch validation errors for client-specific keystore keys
        final List<Setting<?>> clientSpecificSettings = createClientSpecificSettings();
        
        final List<Setting<?>> allSettings = new ArrayList<>(baseSettings);
        allSettings.addAll(clientSpecificSettings);
        return allSettings;
    }
    
    /**
     * Creates client-specific keystore settings for common client names.
     * This allows OpenSearch to recognize keystore keys like:
     * - aiven.gcs.client.myclient.credentials_file
     * - aiven.gcs.client.client2.credentials_file
     * etc.
     * Note: Only credentials_file is stored as client-specific in the keystore.
     * Public and private keys use the default keystore keys.
     */
    private List<Setting<?>> createClientSpecificSettings() {
        final List<Setting<?>> settings = new ArrayList<>();
        
        // Register keystore settings for common client names
        // OpenSearch doesn't support wildcards in setting names, so we register explicitly
        // This list can be extended as needed for new clients
        final String[] commonClientNames = {
            "aiven_btar",  // Your specific client
            "client1",     // Generic examples
            "client2",
            "myclient",
            "testclient"   // From test
        };
        
        for (final String clientName : commonClientNames) {
            final var clientCredentialsSetting = 
                SecureSetting.secureFile("aiven.gcs.client." + clientName + ".credentials_file", null);
            settings.add(clientCredentialsSetting);
        }
        
        return settings;
    }

}
