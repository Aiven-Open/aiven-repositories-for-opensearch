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

import java.io.InputStream;

import io.aiven.elasticsearch.repositories.CommonSettings.KeystoreSettings;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import static io.aiven.elasticsearch.repositories.CommonSettings.KeystoreSettings.checkSettings;
import static io.aiven.elasticsearch.repositories.CommonSettings.KeystoreSettings.withPrefix;

public class S3StorageSettings implements KeystoreSettings {

    public static final Setting<InputStream> PUBLIC_KEY_FILE =
            SecureSetting.secureFile(withPrefix("s3.public_key_file"), null);

    public static final Setting<InputStream> PRIVATE_KEY_FILE =
            SecureSetting.secureFile(withPrefix("s3.private_key_file"), null);

    public static final Setting<SecureString> AWS_SECRET_ACCESS_KEY =
            SecureSetting.secureString(withPrefix("s3.client.aws_secret_access_key"), null);

    public static final Setting<SecureString> AWS_ACCESS_KEY_ID =
            SecureSetting.secureString(withPrefix("s3.client.aws_access_key_id"), null);

    public static final Setting<SecureString> ENDPOINT =
            SecureSetting.secureString(withPrefix("s3.client.endpoint"), null);

    public static final Setting<String> PROXY_HOST =
            Setting.simpleString(withPrefix("s3.client.proxy.host"), Setting.Property.NodeScope);

    public static final Setting<Integer> PROXY_PORT =
            SecureSetting.intSetting(withPrefix("s3.client.proxy.port"), DEFAULT_SOCKS5_PORT, 0,
                    Setting.Property.NodeScope);

    public static final Setting<SecureString> PROXY_USER_NAME =
            SecureSetting.secureString(withPrefix("s3.client.proxy.user_name"), null);

    public static final Setting<SecureString> PROXY_USER_PASSWORD =
            SecureSetting.secureString(withPrefix("s3.client.proxy.user_password"), null);

    public static final Setting<Integer> MAX_RETRIES =
            Setting.intSetting(
                    withPrefix("s3.client.max_retries"),
                    ClientConfiguration.DEFAULT_RETRY_POLICY.getMaxErrorRetry(),
                    Setting.Property.NodeScope);

    public static final Setting<Boolean> USE_THROTTLE_RETRIES =
            Setting.boolSetting(
                    withPrefix("s3.client.use_throttle_retries"),
                    ClientConfiguration.DEFAULT_THROTTLE_RETRIES,
                    Setting.Property.NodeScope);

    public static final Setting<TimeValue> READ_TIMEOUT =
            Setting.timeSetting(
                    withPrefix("s3.client.read_timeout"),
                    TimeValue.timeValueMillis(ClientConfiguration.DEFAULT_SOCKET_TIMEOUT),
                    Setting.Property.NodeScope);

    private final InputStream publicKey;

    private final InputStream privateKey;

    private final AWSCredentials awsCredentials;

    private final String endpoint;

    private final int maxRetries;

    private final boolean useThrottleRetries;

    private final long readTimeout;

    private final String proxyHost;

    private final int proxyPort;

    private final String proxyUsername;

    private final char[] proxyUserPassword;

    private S3StorageSettings(
            final InputStream publicKey,
            final InputStream privateKey,
            final AWSCredentials awsCredentials,
            final String endpoint,
            final int maxRetries,
            final boolean useThrottleRetries,
            final long readTimeout,
            final String proxyHost,
            final int proxyPort,
            final String proxyUsername,
            final char[] proxyUserPassword) {
        this.publicKey = publicKey;
        this.privateKey = privateKey;
        this.awsCredentials = awsCredentials;
        this.endpoint = endpoint;
        this.maxRetries = maxRetries;
        this.useThrottleRetries = useThrottleRetries;
        this.readTimeout = readTimeout;
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.proxyUsername = proxyUsername;
        this.proxyUserPassword = proxyUserPassword;
    }

    public InputStream publicKey() {
        return publicKey;
    }

    public InputStream privateKey() {
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

    public String proxyHost() {
        return proxyHost;
    }

    public int proxyPort() {
        return proxyPort;
    }

    public String proxyUsername() {
        return proxyUsername;
    }

    public char[] proxyUserPassword() {
        return proxyUserPassword;
    }

    public static S3StorageSettings create(final Settings settings) {
        if (settings.isEmpty()) {
            throw new IllegalArgumentException("Settings for AWS S3 haven't been set");
        }
        checkSettings(AWS_ACCESS_KEY_ID, settings);
        checkSettings(AWS_SECRET_ACCESS_KEY, settings);
        checkSettings(ENDPOINT, settings);
        checkSettings(PUBLIC_KEY_FILE, settings);
        checkSettings(PRIVATE_KEY_FILE, settings);
        if (PROXY_PORT.exists(settings) && PROXY_PORT.get(settings) < 0) {
            throw new IllegalArgumentException("Settings with name " + PROXY_PORT.getKey() + " must be greater than 0");
        }
        return new S3StorageSettings(
                PUBLIC_KEY_FILE.get(settings),
                PRIVATE_KEY_FILE.get(settings),
                new BasicAWSCredentials(
                        AWS_ACCESS_KEY_ID.get(settings).toString(),
                        AWS_SECRET_ACCESS_KEY.get(settings).toString()
                ),
                ENDPOINT.get(settings).toString(),
                MAX_RETRIES.get(settings),
                USE_THROTTLE_RETRIES.get(settings),
                READ_TIMEOUT.get(settings).millis(),
                PROXY_HOST.get(settings),
                PROXY_PORT.get(settings),
                PROXY_USER_NAME.get(settings).toString(),
                PROXY_USER_PASSWORD.get(settings).getChars());
    }

}
