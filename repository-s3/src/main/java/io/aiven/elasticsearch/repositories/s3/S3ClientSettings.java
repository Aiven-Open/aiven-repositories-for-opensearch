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

package io.aiven.elasticsearch.repositories.s3;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import io.aiven.elasticsearch.repositories.CommonSettings.ClientSettings;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import static io.aiven.elasticsearch.repositories.CommonSettings.ClientSettings.checkSettings;
import static io.aiven.elasticsearch.repositories.CommonSettings.ClientSettings.getConfigValue;
import static io.aiven.elasticsearch.repositories.CommonSettings.ClientSettings.readInputStream;

public class S3ClientSettings implements ClientSettings {

    static final String S3_PREFIX = AIVEN_PREFIX + "s3.";

    public static final Setting.AffixSetting<InputStream> PUBLIC_KEY_FILE =
            Setting.affixKeySetting(
                    S3_PREFIX,
                    "public_key_file",
                    key -> SecureSetting.secureFile(key, null)
            );

    public static final Setting.AffixSetting<InputStream> PRIVATE_KEY_FILE =
            Setting.affixKeySetting(
                    S3_PREFIX,
                    "private_key_file",
                    key -> SecureSetting.secureFile(key, null)
            );

    public static final Setting.AffixSetting<SecureString> AWS_SECRET_ACCESS_KEY =
            Setting.affixKeySetting(
                    S3_PREFIX,
                    "client.aws_secret_access_key",
                    key -> SecureSetting.secureString(key, null)
            );

    public static final Setting.AffixSetting<SecureString> AWS_ACCESS_KEY_ID =
            Setting.affixKeySetting(
                    S3_PREFIX,
                    "client.aws_access_key_id",
                    key -> SecureSetting.secureString(key, null)
            );

    public static final Setting.AffixSetting<SecureString> ENDPOINT =
            Setting.affixKeySetting(
                    S3_PREFIX,
                    "client.endpoint",
                    key -> SecureSetting.secureString(key, null)
            );

    public static final Setting.AffixSetting<Integer> MAX_RETRIES =
            Setting.affixKeySetting(
                    S3_PREFIX,
                    "client.max_retries",
                    key -> Setting.intSetting(key,
                            ClientConfiguration.DEFAULT_RETRY_POLICY.getMaxErrorRetry(), Setting.Property.NodeScope)
            );

    public static final Setting.AffixSetting<Boolean> USE_THROTTLE_RETRIES =
            Setting.affixKeySetting(
                    S3_PREFIX,
                    "client.use_throttle_retries",
                    key -> Setting.boolSetting(key,
                            ClientConfiguration.DEFAULT_THROTTLE_RETRIES, Setting.Property.NodeScope)
            );

    public static final Setting.AffixSetting<TimeValue> READ_TIMEOUT =
            Setting.affixKeySetting(
                    S3_PREFIX,
                    "client.read_timeout",
                    key -> Setting.timeSetting(key,
                            TimeValue.timeValueMillis(ClientConfiguration.DEFAULT_SOCKET_TIMEOUT),
                            Setting.Property.NodeScope)
            );

    private final byte[] publicKey;

    private final byte[] privateKey;

    private final AWSCredentials awsCredentials;

    private final String endpoint;

    private final int maxRetries;

    private final boolean useThrottleRetries;

    private final long readTimeout;

    private S3ClientSettings(
            final byte[] publicKey,
            final byte[] privateKey,
            final AWSCredentials awsCredentials,
            final String endpoint,
            final int maxRetries,
            final boolean useThrottleRetries,
            final long readTimeout) {
        this.publicKey = publicKey;
        this.privateKey = privateKey;
        this.awsCredentials = awsCredentials;
        this.endpoint = endpoint;
        this.maxRetries = maxRetries;
        this.useThrottleRetries = useThrottleRetries;
        this.readTimeout = readTimeout;
    }

    @Override
    public byte[] publicKey() {
        return publicKey;
    }

    @Override
    public byte[] privateKey() {
        return privateKey;
    }

    public AWSCredentials awsCredentials() {
        return awsCredentials;
    }

    public String endpoint() {
        return endpoint;
    }

    public int maxRetries() {
        return maxRetries;
    }

    public boolean useThrottleRetries() {
        return useThrottleRetries;
    }

    public int readTimeout() {
        return Math.toIntExact(readTimeout);
    }

    public static Map<String, S3ClientSettings> create(final Settings settings) throws IOException {
        if (settings.isEmpty()) {
            throw new IllegalArgumentException("Settings for AWS S3 haven't been set");
        }
        final Set<String> clientNames = settings.getGroups(S3_PREFIX).keySet();
        final var clientSettings = new HashMap<String, S3ClientSettings>();
        for (final var clientName : clientNames) {
            clientSettings.put(clientName, createSettings(clientName, settings));
        }
        return Map.copyOf(clientSettings);
    }

    static S3ClientSettings createSettings(final String clientName, final Settings settings) throws IOException {
        checkSettings(AWS_ACCESS_KEY_ID, clientName, settings);
        checkSettings(AWS_SECRET_ACCESS_KEY, clientName, settings);
        checkSettings(ENDPOINT, clientName, settings);
        checkSettings(PUBLIC_KEY_FILE, clientName, settings);
        checkSettings(PRIVATE_KEY_FILE, clientName, settings);
        return new S3ClientSettings(
                readInputStream(getConfigValue(settings, clientName, PUBLIC_KEY_FILE)),
                readInputStream(getConfigValue(settings, clientName, PRIVATE_KEY_FILE)),
                new BasicAWSCredentials(
                        getConfigValue(settings, clientName, AWS_ACCESS_KEY_ID).toString(),
                        getConfigValue(settings, clientName, AWS_SECRET_ACCESS_KEY).toString()
                ),
                getConfigValue(settings, clientName, ENDPOINT).toString(),
                getConfigValue(settings, clientName, MAX_RETRIES),
                getConfigValue(settings, clientName, USE_THROTTLE_RETRIES),
                getConfigValue(settings, clientName, READ_TIMEOUT).millis()
        );
    }

}
