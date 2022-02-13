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
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import io.aiven.elasticsearch.repositories.CommonSettings;

import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import static io.aiven.elasticsearch.repositories.CommonSettings.ClientSettings.checkSettings;
import static io.aiven.elasticsearch.repositories.CommonSettings.ClientSettings.getConfigValue;
import static io.aiven.elasticsearch.repositories.CommonSettings.ClientSettings.readInputStream;

public class AzureClientSettings implements CommonSettings.ClientSettings {

    static final String AZURE_PREFIX = AIVEN_PREFIX + "azure.";

    static final String AZURE_CONNECTION_STRING_TEMPLATE =
            "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s";

    static final Setting.AffixSetting<Integer> AZURE_HTTP_POOL_MIN_THREADS =
            Setting.affixKeySetting(
                    AZURE_PREFIX,
                    "http.thread_pool.min",
                    key -> Setting.intSetting(key,
                            Runtime.getRuntime().availableProcessors() * 2 - 1, 1,
                            Setting.Property.NodeScope)
            );

    static final Setting.AffixSetting<Integer> AZURE_HTTP_POOL_MAX_THREADS =
            Setting.affixKeySetting(
                    AZURE_PREFIX,
                    "http.thread_pool.max",
                    key -> Setting.intSetting(key,
                            Runtime.getRuntime().availableProcessors() * 2 - 1, 1,
                            Setting.Property.NodeScope)
            );

    static final Setting.AffixSetting<TimeValue> AZURE_HTTP_POOL_KEEP_ALIVE =
            Setting.affixKeySetting(
                    AZURE_PREFIX,
                    "http.thread_pool.keep_alive",
                    key -> Setting.timeSetting(key, TimeValue.timeValueSeconds(30L), Setting.Property.NodeScope)
            );

    static final Setting.AffixSetting<Integer> AZURE_HTTP_POOL_WORKING_QUEUE_SIZE =
            Setting.affixKeySetting(
                    AZURE_PREFIX,
                    "http.thread_pool.working_queue_size",
                    key -> Setting.intSetting(key, 1000, 10, Setting.Property.NodeScope)
            );


    public static final Setting.AffixSetting<InputStream> PUBLIC_KEY_FILE =
            Setting.affixKeySetting(
                    AZURE_PREFIX,
                    "public_key_file",
                    key -> SecureSetting.secureFile(key, null)
            );

    public static final Setting.AffixSetting<InputStream> PRIVATE_KEY_FILE =
            Setting.affixKeySetting(
                    AZURE_PREFIX,
                    "private_key_file",
                    key -> SecureSetting.secureFile(key, null)
            );

    public static final Setting.AffixSetting<SecureString> AZURE_ACCOUNT =
            Setting.affixKeySetting(
                    AZURE_PREFIX,
                    "client.account",
                    key -> SecureSetting.secureString(key, null)
            );

    public static final Setting.AffixSetting<SecureString> AZURE_ACCOUNT_KEY =
            Setting.affixKeySetting(
                    AZURE_PREFIX,
                    "client.account.key",
                    key -> SecureSetting.secureString(key, null)
            );

    //default is 3 please take a look ExponentialBackoff azure class
    public static final Setting.AffixSetting<Integer> MAX_RETRIES =
            Setting.affixKeySetting(
                    AZURE_PREFIX,
                    "max_retries",
                    key -> Setting.intSetting(key, 3, Setting.Property.NodeScope)
            );

    private final byte[] publicKey;

    private final byte[] privateKey;

    private final String azureAccount;

    private final String azureAccountKey;

    private final int maxRetries;

    private final HttpThreadPoolSettings httpThreadPoolSettings;

    AzureClientSettings(final byte[] publicKey,
                        final byte[] privateKey,
                        final String azureAccount,
                        final String azureAccountKey,
                        final int maxRetries,
                        final HttpThreadPoolSettings httpThreadPoolSettings) {
        this.publicKey = publicKey;
        this.privateKey = privateKey;
        this.azureAccount = azureAccount;
        this.azureAccountKey = azureAccountKey;
        this.maxRetries = maxRetries;
        this.httpThreadPoolSettings = httpThreadPoolSettings;
    }

    public byte[] publicKey() {
        return publicKey;
    }

    public byte[] privateKey() {
        return privateKey;
    }

    public String azureConnectionString() {
        return String.format(AZURE_CONNECTION_STRING_TEMPLATE, azureAccount(), azureAccountKey());
    }

    public String azureAccount() {
        return azureAccount;
    }

    public String azureAccountKey() {
        return azureAccountKey;
    }

    public int maxRetries() {
        return maxRetries;
    }

    public static Map<String, AzureClientSettings> create(final Settings settings) throws IOException {
        final Set<String> clientNames = settings.getGroups(AZURE_PREFIX).keySet();
        final var clientSettings = new HashMap<String, AzureClientSettings>();
        for (final var clientName : clientNames) {
            clientSettings.put(clientName, createSettings(clientName, settings));
        }
        return Map.copyOf(clientSettings);
    }

    static AzureClientSettings createSettings(final String clientName, final Settings settings) throws IOException {
        checkSettings(AZURE_ACCOUNT, clientName, settings);
        checkSettings(AZURE_ACCOUNT_KEY, clientName, settings);
        checkSettings(PUBLIC_KEY_FILE, clientName, settings);
        checkSettings(PRIVATE_KEY_FILE, clientName, settings);
        return new AzureClientSettings(
                readInputStream(getConfigValue(settings, clientName, PUBLIC_KEY_FILE)),
                readInputStream(getConfigValue(settings, clientName, PRIVATE_KEY_FILE)),
                getConfigValue(settings, clientName, AZURE_ACCOUNT).toString(),
                getConfigValue(settings, clientName, AZURE_ACCOUNT_KEY).toString(),
                getConfigValue(settings, clientName, MAX_RETRIES),
                new HttpThreadPoolSettings(
                        getConfigValue(settings, clientName, AZURE_HTTP_POOL_MIN_THREADS),
                        getConfigValue(settings, clientName, AZURE_HTTP_POOL_MAX_THREADS),
                        getConfigValue(settings, clientName, AZURE_HTTP_POOL_KEEP_ALIVE).getMillis(),
                        getConfigValue(settings, clientName, AZURE_HTTP_POOL_WORKING_QUEUE_SIZE))
        );
    }

    public HttpThreadPoolSettings httpThreadPoolSettings() {
        return httpThreadPoolSettings;
    }

    static final class HttpThreadPoolSettings {

        private final int minThreads;

        private final int maxThreads;

        private final long keepAlive;

        private final int workingQueueSize;

        private HttpThreadPoolSettings(final int minThreads,
                                       final int maxThreads,
                                       final long keepAlive,
                                       final int workingQueueSize) {
            this.minThreads = minThreads;
            this.maxThreads = maxThreads;
            this.keepAlive = keepAlive;
            this.workingQueueSize = workingQueueSize;
        }

        public int minThreads() {
            return minThreads;
        }

        public int maxThreads() {
            return maxThreads;
        }

        public long keepAlive() {
            return keepAlive;
        }

        public int workingQueueSize() {
            return workingQueueSize;
        }

    }

}
