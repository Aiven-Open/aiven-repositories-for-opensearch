/*
 * Copyright 2026 Aiven Oy
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

package io.aiven.elasticsearch.crypto.provider;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.security.KeyPair;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.CryptoKeyProviderPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ReloadablePlugin;

/**
 * Key provider of type {@value #TYPE}. The {@code key_provider_name} of a repository's {@code crypto_settings}
 * selects the RSA key pair stored in the keystore as {@code aiven.crypto.<key_provider_name>.public_key_file}
 * and {@code aiven.crypto.<key_provider_name>.private_key_file}.
 */
public class RsaKeyProviderPlugin extends Plugin implements CryptoKeyProviderPlugin, ReloadablePlugin {

    public static final String TYPE = "aiven-rsa";

    static final String PREFIX = "aiven.crypto.";

    static final Setting.AffixSetting<InputStream> PUBLIC_KEY_FILE =
        Setting.affixKeySetting(PREFIX, "public_key_file", key -> SecureSetting.secureFile(key, null));

    static final Setting.AffixSetting<InputStream> PRIVATE_KEY_FILE =
        Setting.affixKeySetting(PREFIX, "private_key_file", key -> SecureSetting.secureFile(key, null));

    private volatile Map<String, KeyPair> keyPairs;

    public RsaKeyProviderPlugin(final Settings settings) {
        reload(settings);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(PUBLIC_KEY_FILE, PRIVATE_KEY_FILE);
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public MasterKeyProvider createKeyProvider(final CryptoMetadata cryptoMetadata) {
        if (!cryptoMetadata.settings().isEmpty()) {
            throw new IllegalArgumentException(
                "Key provider type [" + TYPE + "] takes no crypto_settings.settings, got " + cryptoMetadata.settings().keySet());
        }
        final var name = cryptoMetadata.keyProviderName();
        final var keyPair = keyPairs.get(name);
        if (keyPair == null) {
            throw new IllegalArgumentException("No RSA key pair for key provider [" + name + "]. Set "
                + PUBLIC_KEY_FILE.getConcreteSettingForNamespace(name).getKey() + " and "
                + PRIVATE_KEY_FILE.getConcreteSettingForNamespace(name).getKey() + " in the keystore");
        }
        return new RsaMasterKeyProvider(keyPair);
    }

    @Override
    public void reload(final Settings settings) {
        final var names = new HashSet<>(PUBLIC_KEY_FILE.getNamespaces(settings));
        names.addAll(PRIVATE_KEY_FILE.getNamespaces(settings));
        final var loaded = new HashMap<String, KeyPair>();
        for (final var name : names) {
            loaded.put(name, RsaKeys.readKeyPair(
                readSecureFile(PUBLIC_KEY_FILE.getConcreteSettingForNamespace(name), settings),
                readSecureFile(PRIVATE_KEY_FILE.getConcreteSettingForNamespace(name), settings)));
        }
        keyPairs = Map.copyOf(loaded);
    }

    private static byte[] readSecureFile(final Setting<InputStream> setting, final Settings settings) {
        if (!setting.exists(settings)) {
            throw new IllegalArgumentException("Keystore setting [" + setting.getKey() + "] is missing");
        }
        try (final var in = setting.get(settings)) {
            return in.readAllBytes();
        } catch (final IOException e) {
            throw new UncheckedIOException("Couldn't read keystore setting [" + setting.getKey() + "]", e);
        }
    }
}
