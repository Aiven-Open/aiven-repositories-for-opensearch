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

package io.aiven.elasticsearch.repositories;

import java.io.IOException;
import java.io.InputStream;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.repositories.RepositoryException;

public interface CommonSettings {

    interface ClientSettings {

        String AIVEN_PREFIX = "aiven.";

        static <T> void checkSettings(
                final Setting.AffixSetting<T> setting,
                String clientName,
                Settings keystoreSettings) {
            if (!setting.getConcreteSettingForNamespace(clientName).exists(keystoreSettings)) {
                throw new IllegalArgumentException("Settings with name "
                        + setting.getConcreteSettingForNamespace(clientName).getKey()
                        + " hasn't been set");
            }
        }

        static byte[] readInputStream(InputStream keyIn) throws IOException {
            try (final var in = keyIn) {
                return in.readAllBytes();
            }
        }

        static <T> T getConfigValue(Settings settings, String clientName, Setting.AffixSetting<T> clientSetting) {
            final Setting<T> concreteSetting = clientSetting.getConcreteSettingForNamespace(clientName);
            return concreteSetting.get(settings);
        }

        byte[] publicKey();

        byte[] privateKey();

    }

    interface RepositorySettings {

        Setting<String> CLIENT_NAME =
                Setting.simpleString(
                        "client",
                        "default",
                        Setting.Property.NodeScope,
                        Setting.Property.Dynamic
                );

        Setting<String> BASE_PATH =
                Setting.simpleString(
                        "base_path",
                        Setting.Property.NodeScope,
                        Setting.Property.Dynamic
                );

        // For default settings of AES/CTR which we use the maximum size of the file
        // with the same IV is 64GB (2^32 * 128 bit per block)
        // to use 5TB maximum storage limit we need to improve our logic and use more than one IV
        // In case of AES/GCM maximum size is teh same since it is extension of CTR with hashing,
        // but from security point of view GCM better
        Setting<ByteSizeValue> CHUNK_SIZE =
                Setting.byteSizeSetting(
                        "chunk_size",
                        new ByteSizeValue(100, ByteSizeUnit.MB),
                        new ByteSizeValue(100, ByteSizeUnit.MB),
                        new ByteSizeValue(64, ByteSizeUnit.GB),
                        Setting.Property.NodeScope,
                        Setting.Property.Dynamic
                );

        /**
         * The number of retries to use when an GCS request fails.
         */
        Setting<Integer> MAX_RETRIES =
                Setting.intSetting(
                        "max_retries", 3,
                        Setting.Property.NodeScope,
                        Setting.Property.Dynamic);


        static void checkSettings(final String repoType, final Setting<String> setting, final Settings settings) {
            if (!setting.exists(settings)) {
                throw new RepositoryException(repoType, setting.getKey() + " hasn't been defined");
            }
        }

    }

}
