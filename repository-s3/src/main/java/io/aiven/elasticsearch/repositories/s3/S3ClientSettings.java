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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.settings.SecureString;

import io.aiven.elasticsearch.repositories.CommonSettings.ClientSettings;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

import static io.aiven.elasticsearch.repositories.CommonSettings.ClientSettings.checkSettings;
import static io.aiven.elasticsearch.repositories.CommonSettings.ClientSettings.getConfigValue;
import static io.aiven.elasticsearch.repositories.CommonSettings.ClientSettings.readInputStream;

public class S3ClientSettings implements ClientSettings {

    static final String S3_PREFIX = AIVEN_PREFIX + "s3.client.";

    public static final Setting.AffixSetting<InputStream> PUBLIC_KEY_FILE =
            Setting.affixKeySetting(
                    S3_PREFIX,
                    "public_key_file",
                    key -> SecureSetting.secureFile(key, LegacyFallback.PUBLIC_KEY_FILE)
            );

    public static final Setting.AffixSetting<InputStream> PRIVATE_KEY_FILE =
            Setting.affixKeySetting(
                    S3_PREFIX,
                    "private_key_file",
                    key -> SecureSetting.secureFile(key, LegacyFallback.PRIVATE_KEY_FILE)
            );

    public static final Setting.AffixSetting<SecureString> AWS_SECRET_ACCESS_KEY =
            Setting.affixKeySetting(
                    S3_PREFIX,
                    "aws_secret_access_key",
                    key -> SecureSetting.secureString(key, LegacyFallback.AWS_SECRET_ACCESS_KEY)
            );

    public static final Setting.AffixSetting<SecureString> AWS_ACCESS_KEY_ID =
            Setting.affixKeySetting(
                    S3_PREFIX,
                    "aws_access_key_id",
                    key -> SecureSetting.secureString(key, LegacyFallback.AWS_ACCESS_KEY_ID)
            );

    public static final Setting.AffixSetting<SecureString> ENDPOINT =
            Setting.affixKeySetting(
                    S3_PREFIX,
                    "endpoint",
                    key -> SecureSetting.secureString(key, LegacyFallback.ENDPOINT)
            );

    public static final Setting.AffixSetting<Integer> MAX_RETRIES =
            Setting.affixKeySetting(
                    S3_PREFIX,
                    "max_retries",
                    key -> Setting.intSetting(key, LegacyFallback.MAX_RETRIES, Setting.Property.NodeScope)
            );

    public static final Setting.AffixSetting<Boolean> USE_THROTTLE_RETRIES =
            Setting.affixKeySetting(
                    S3_PREFIX,
                    "use_throttle_retries",
                    key -> Setting.boolSetting(key, LegacyFallback.USE_THROTTLE_RETRIES, Setting.Property.NodeScope)
            );

    public static final Setting.AffixSetting<TimeValue> READ_TIMEOUT =
            Setting.affixKeySetting(
                    S3_PREFIX,
                    "read_timeout",
                    key -> Setting.timeSetting(key, LegacyFallback.READ_TIMEOUT, Setting.Property.NodeScope)
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

        final var clientSettings = new HashMap<String, S3ClientSettings>();
        final var filteredSettings = settings.filter(key -> !LegacyFallback.KEY_MAP.containsKey(key));
        final Set<String> clientNames = filteredSettings.getGroups(S3_PREFIX).keySet();
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
        return getConfigValue(settings, DEFAULT_CLIENT_NAME, AWS_ACCESS_KEY_ID) != null
               || getConfigValue(settings, DEFAULT_CLIENT_NAME, AWS_SECRET_ACCESS_KEY) != null
               || getConfigValue(settings, DEFAULT_CLIENT_NAME, ENDPOINT) != null
               || getConfigValue(settings, DEFAULT_CLIENT_NAME, PUBLIC_KEY_FILE) != null
               || getConfigValue(settings, DEFAULT_CLIENT_NAME, PRIVATE_KEY_FILE) != null;
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

    public static class LegacyFallback {
        static final Setting<InputStream> PUBLIC_KEY_FILE =
            SecureSetting.secureFile("aiven.s3.public_key_file", null);
        static final Setting<InputStream> PRIVATE_KEY_FILE =
            SecureSetting.secureFile("aiven.s3.private_key_file", null);
        static final Setting<SecureString> AWS_SECRET_ACCESS_KEY =
            SecureSetting.secureString("aiven.s3.client.aws_secret_access_key", null);
        static final Setting<SecureString> AWS_ACCESS_KEY_ID =
            SecureSetting.secureString("aiven.s3.client.aws_access_key_id", null);
        static final Setting<SecureString> ENDPOINT =
            SecureSetting.secureString("aiven.s3.client.endpoint", null);
        public static final Setting<Integer> MAX_RETRIES =
            Setting.intSetting("aiven.s3.client.max_retries", 3, Setting.Property.NodeScope);
        public static final Setting<Boolean> USE_THROTTLE_RETRIES =
            Setting.boolSetting("aiven.s3.client.use_throttle_retries", true, Setting.Property.NodeScope);
        public static final Setting<TimeValue> READ_TIMEOUT =
            Setting.timeSetting("aiven.s3.client.read_timeout", TimeValue.timeValueMillis(50 * 1000),
                                Setting.Property.NodeScope);

        public static final Map<String, Setting<?>> KEY_MAP;

        static {
            KEY_MAP = Stream.of(PUBLIC_KEY_FILE, PRIVATE_KEY_FILE, AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID, ENDPOINT,
                               MAX_RETRIES, USE_THROTTLE_RETRIES, READ_TIMEOUT)
                            .collect(Collectors.toMap(Setting::getKey, s -> s));
        }
    }
}
