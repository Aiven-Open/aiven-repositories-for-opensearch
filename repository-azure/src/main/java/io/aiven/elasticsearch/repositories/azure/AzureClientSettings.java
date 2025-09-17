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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.SecureString;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import io.aiven.elasticsearch.repositories.CommonSettings;

import static io.aiven.elasticsearch.repositories.CommonSettings.ClientSettings.checkSettings;
import static io.aiven.elasticsearch.repositories.CommonSettings.ClientSettings.getConfigValue;
import static io.aiven.elasticsearch.repositories.CommonSettings.ClientSettings.readInputStream;
import static org.opensearch.common.settings.SecureSetting.secureFile;
import static org.opensearch.common.settings.SecureSetting.secureString;
import static org.opensearch.common.settings.Setting.intSetting;

public class AzureClientSettings implements CommonSettings.ClientSettings {

    static final String AZURE_PREFIX = AIVEN_PREFIX + "azure.client.";

    static final String AZURE_CONNECTION_STRING_TEMPLATE =
            "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s";

    static final Setting.AffixSetting<Integer> AZURE_HTTP_POOL_MIN_THREADS =
            Setting.affixKeySetting(
                    AZURE_PREFIX,
                    "http.thread_pool.min",
                    key -> intSetting(key,
                            LegacyFallback.AZURE_HTTP_POOL_MIN_THREADS,
                            Setting.Property.NodeScope)
            );

    static final Setting.AffixSetting<Integer> AZURE_HTTP_POOL_MAX_THREADS =
            Setting.affixKeySetting(
                    AZURE_PREFIX,
                    "http.thread_pool.max",
                    key -> intSetting(key,
                            LegacyFallback.AZURE_HTTP_POOL_MAX_THREADS,
                            Setting.Property.NodeScope)
            );

    static final Setting.AffixSetting<TimeValue> AZURE_HTTP_POOL_KEEP_ALIVE =
            Setting.affixKeySetting(
                    AZURE_PREFIX,
                    "http.thread_pool.keep_alive",
                    key -> Setting.timeSetting(key,
                                               LegacyFallback.AZURE_HTTP_POOL_KEEP_ALIVE,
                                               Setting.Property.NodeScope)
            );

    static final Setting.AffixSetting<Integer> AZURE_HTTP_POOL_WORKING_QUEUE_SIZE =
            Setting.affixKeySetting(
                    AZURE_PREFIX,
                    "http.thread_pool.working_queue_size",
                    key -> intSetting(key, LegacyFallback.AZURE_HTTP_POOL_WORKING_QUEUE_SIZE, 10, Setting.Property.NodeScope)
            );


    public static final Setting.AffixSetting<InputStream> PUBLIC_KEY_FILE =
            Setting.affixKeySetting(
                    AZURE_PREFIX,
                    "public_key_file",
                    key -> secureFile(key, LegacyFallback.PUBLIC_KEY_FILE)
            );

    public static final Setting.AffixSetting<InputStream> PRIVATE_KEY_FILE =
            Setting.affixKeySetting(
                    AZURE_PREFIX,
                    "private_key_file",
                    key -> secureFile(key, LegacyFallback.PRIVATE_KEY_FILE)
            );

    public static final Setting.AffixSetting<SecureString> AZURE_ACCOUNT =
            Setting.affixKeySetting(
                    AZURE_PREFIX,
                    "account",
                    key -> secureString(key, LegacyFallback.AZURE_ACCOUNT)
            );

    public static final Setting.AffixSetting<SecureString> AZURE_ACCOUNT_KEY =
            Setting.affixKeySetting(
                    AZURE_PREFIX,
                    "account.key",
                    key -> secureString(key, LegacyFallback.AZURE_ACCOUNT_KEY)
            );

    //default is 3 please take a look ExponentialBackoff azure class
    public static final Setting.AffixSetting<Integer> MAX_RETRIES =
            Setting.affixKeySetting(
                    AZURE_PREFIX,
                    "max_retries",
                    key -> intSetting(key, LegacyFallback.MAX_RETRIES, Setting.Property.NodeScope)
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
        if (settings.isEmpty()) {
            throw new IllegalArgumentException("Settings for Azure haven't been set");
        }

        final var clientSettings = new HashMap<String, AzureClientSettings>();
        final var filteredSettings = settings.filter(key -> !LegacyFallback.KEY_MAP.containsKey(key));
        final Set<String> clientNames = filteredSettings.getGroups(AZURE_PREFIX).keySet();
        for (final var clientName : clientNames) {
            clientSettings.put(clientName, createSettings(clientName, settings));
        }

        if (!clientSettings.containsKey(DEFAULT_CLIENT_NAME) && hasLegacyDefaultOrLegacyCredentials(settings)) {
            // this won't find any settings under the default client,
            // but it will pull all the fallback static settings
            clientSettings.put(DEFAULT_CLIENT_NAME, createSettings(DEFAULT_CLIENT_NAME, settings));
        }

        return Map.copyOf(clientSettings);
    }

    private static boolean hasLegacyDefaultOrLegacyCredentials(final Settings settings) {
        return getConfigValue(settings, DEFAULT_CLIENT_NAME, AZURE_ACCOUNT) != null
               || getConfigValue(settings, DEFAULT_CLIENT_NAME, AZURE_ACCOUNT_KEY) != null
               || getConfigValue(settings, DEFAULT_CLIENT_NAME, PUBLIC_KEY_FILE) != null
               || getConfigValue(settings, DEFAULT_CLIENT_NAME, PRIVATE_KEY_FILE) != null;
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

    public static class LegacyFallback {
        static final Setting<Integer> AZURE_HTTP_POOL_MIN_THREADS =
            Setting.intSetting("aiven.azure.http.thread_pool.min",
                               Runtime.getRuntime().availableProcessors() * 2 - 1, 1,
                               Setting.Property.NodeScope);
        static final Setting<Integer> AZURE_HTTP_POOL_MAX_THREADS =
            Setting.intSetting("aiven.azure.http.thread_pool.max",
                               Runtime.getRuntime().availableProcessors() * 2 - 1, 1,
                               Setting.Property.NodeScope);
        static final Setting<TimeValue> AZURE_HTTP_POOL_KEEP_ALIVE =
            Setting.timeSetting("aiven.azure.http.thread_pool.keep_alive", TimeValue.timeValueSeconds(30L),
                                Setting.Property.NodeScope);
        static final Setting<Integer> AZURE_HTTP_POOL_WORKING_QUEUE_SIZE =
            Setting.intSetting("aiven.azure.http.thread_pool.working_queue_size", 1000, 10,
                               Setting.Property.NodeScope);
        static final Setting<InputStream> PUBLIC_KEY_FILE =
            SecureSetting.secureFile("aiven.azure.public_key_file", null);
        static final Setting<InputStream> PRIVATE_KEY_FILE =
            SecureSetting.secureFile("aiven.azure.private_key_file", null);
        static final Setting<SecureString> AZURE_ACCOUNT =
            SecureSetting.secureString("aiven.azure.client.account", null);
        static final Setting<SecureString> AZURE_ACCOUNT_KEY =
            SecureSetting.secureString("aiven.azure.client.account.key", null);
        static final Setting<Integer> MAX_RETRIES =
            Setting.intSetting("aiven.azure.max_retries", 3, Setting.Property.NodeScope);

        public static final Map<String, Setting<?>> KEY_MAP;

        static {
            KEY_MAP = Stream.of(AZURE_HTTP_POOL_MIN_THREADS, AZURE_HTTP_POOL_MAX_THREADS,
                               AZURE_HTTP_POOL_KEEP_ALIVE, AZURE_HTTP_POOL_WORKING_QUEUE_SIZE,
                               PUBLIC_KEY_FILE, PRIVATE_KEY_FILE,
                               AZURE_ACCOUNT, AZURE_ACCOUNT_KEY,
                               MAX_RETRIES)
                            .collect(Collectors.toMap(Setting::getKey, s -> s));
        }
    }
}
