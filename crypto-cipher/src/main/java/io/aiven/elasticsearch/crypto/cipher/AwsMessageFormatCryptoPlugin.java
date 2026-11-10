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

package io.aiven.elasticsearch.crypto.cipher;

import java.util.concurrent.TimeUnit;

import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.plugins.CryptoPlugin;
import org.opensearch.plugins.Plugin;

import com.amazonaws.encryptionsdk.CryptoMaterialsManager;
import com.amazonaws.encryptionsdk.caching.CachingCryptoMaterialsManager;
import com.amazonaws.encryptionsdk.caching.LocalCryptoMaterialsCache;

/**
 * Encrypts repositories that set {@code crypto_settings} in the AWS Encryption SDK message format.
 * Core's crypto handler registry keeps one handler per {@code crypto_settings}, so every call creates a new one.
 */
public class AwsMessageFormatCryptoPlugin extends Plugin implements CryptoPlugin<MessageHeader, MessageHeader> {

    /** Encryption reuses a data key for this long. Decryption keeps unwrapped data keys for this long. */
    static final long DATA_KEY_MAX_AGE_MINUTES = 60;

    static final int DATA_KEY_CACHE_SIZE = 1000;

    @Override
    public CryptoHandler<MessageHeader, MessageHeader> getOrCreateCryptoHandler(final MasterKeyProvider keyProvider,
                                                                                final String keyProviderName,
                                                                                final String keyProviderType,
                                                                                final Runnable onClose) {
        return new AwsMessageFormatCryptoHandler(materialsManager(keyProvider, keyProviderName), onClose);
    }

    static CryptoMaterialsManager materialsManager(final MasterKeyProvider keyProvider, final String keyProviderName) {
        return CachingCryptoMaterialsManager.newBuilder()
            .withMasterKeyProvider(new CryptoMasterKey(keyProvider, keyProviderName))
            .withCache(new LocalCryptoMaterialsCache(DATA_KEY_CACHE_SIZE))
            .withMaxAge(DATA_KEY_MAX_AGE_MINUTES, TimeUnit.MINUTES)
            .build();
    }
}
