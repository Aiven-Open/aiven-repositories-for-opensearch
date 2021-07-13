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

import java.io.InputStream;

import io.aiven.elasticsearch.repositories.CommonSettings;

import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.SecureString;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import static io.aiven.elasticsearch.repositories.CommonSettings.KeystoreSettings.checkSettings;
import static io.aiven.elasticsearch.repositories.CommonSettings.KeystoreSettings.withPrefix;

public class AzureStorageSettings implements CommonSettings.KeystoreSettings {

    static final String AZURE_CONNECTION_STRING_TEMPLATE =
            "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s";

    static final Setting<Integer> AZURE_HTTP_POOL_MIN_THREADS =
            Setting.intSetting(withPrefix("azure.http.thread_pool.min"), 1, Setting.Property.NodeScope);

    static final Setting<Integer> AZURE_HTTP_POOL_MAX_THREADS =
            Setting.intSetting(withPrefix("azure.http.thread_pool.max"),
                    Runtime.getRuntime().availableProcessors() * 2 - 1, 1, Setting.Property.NodeScope);

    static final Setting<TimeValue> AZURE_HTTP_POOL_KEEP_ALIVE =
            Setting.timeSetting(withPrefix("azure.http.thread_pool.keep_alive"), TimeValue.timeValueSeconds(30L),
                    Setting.Property.NodeScope);


    public static final Setting<InputStream> PUBLIC_KEY_FILE =
            SecureSetting.secureFile(withPrefix("azure.public_key_file"), null);

    public static final Setting<InputStream> PRIVATE_KEY_FILE =
            SecureSetting.secureFile(withPrefix("azure.private_key_file"), null);

    public static final Setting<SecureString> AZURE_ACCOUNT =
            SecureSetting.secureString(withPrefix("azure.client.account"), null);

    public static final Setting<SecureString> AZURE_ACCOUNT_KEY =
            SecureSetting.secureString(withPrefix("azure.client.account.key"), null);

    //default is 3 please take a look ExponentialBackoff azure class
    public static final Setting<Integer> MAX_RETRIES =
            Setting.intSetting(withPrefix("max_retries"), 3, Setting.Property.NodeScope);

    private final InputStream publicKey;

    private final InputStream privateKey;

    private final String azureAccount;

    private final String azureAccountKey;

    private final int maxRetries;

    private final HttpThreadPoolSettings httpThreadPoolSettings;

    AzureStorageSettings(final InputStream publicKey,
                         final InputStream privateKey,
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

    public InputStream publicKey() {
        return publicKey;
    }

    public InputStream privateKey() {
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

    public static AzureStorageSettings create(final Settings settings) {
        checkSettings(AZURE_ACCOUNT, settings);
        checkSettings(AZURE_ACCOUNT_KEY, settings);
        checkSettings(PUBLIC_KEY_FILE, settings);
        checkSettings(PRIVATE_KEY_FILE, settings);
        return new AzureStorageSettings(
                PUBLIC_KEY_FILE.get(settings),
                PRIVATE_KEY_FILE.get(settings),
                AZURE_ACCOUNT.get(settings).toString(),
                AZURE_ACCOUNT_KEY.get(settings).toString(),
                MAX_RETRIES.get(settings),
                new HttpThreadPoolSettings(
                        AZURE_HTTP_POOL_MIN_THREADS.get(settings),
                        AZURE_HTTP_POOL_MAX_THREADS.get(settings),
                        AZURE_HTTP_POOL_KEEP_ALIVE.get(settings).getMillis())
        );
    }

    public HttpThreadPoolSettings httpThreadPoolSettings() {
        return httpThreadPoolSettings;
    }

    static final class HttpThreadPoolSettings {

        private final int minThreads;

        private final int maxThreads;

        private final long keepAlive;

        private HttpThreadPoolSettings(final int minThreads, final int maxThreads, final long keepAlive) {
            this.minThreads = minThreads;
            this.maxThreads = maxThreads;
            this.keepAlive = keepAlive;
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
    }

}
