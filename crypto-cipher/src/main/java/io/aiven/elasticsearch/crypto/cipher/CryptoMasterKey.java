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

import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;

import org.opensearch.common.crypto.MasterKeyProvider;

import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.DataKey;
import com.amazonaws.encryptionsdk.EncryptedDataKey;
import com.amazonaws.encryptionsdk.MasterKey;

/**
 * Adapts an OpenSearch {@link MasterKeyProvider} to the AWS Encryption SDK {@link MasterKey}. The encrypted data key
 * carries {@code key_provider_name} as its provider id and the key provider's key id as its provider info, which
 * are the key namespace and key name of an AWS Encryption SDK raw keyring.
 */
final class CryptoMasterKey extends MasterKey<CryptoMasterKey> {

    private final MasterKeyProvider keyProvider;

    private final String keyProviderName;

    CryptoMasterKey(final MasterKeyProvider keyProvider, final String keyProviderName) {
        this.keyProvider = keyProvider;
        this.keyProviderName = keyProviderName;
    }

    @Override
    public String getProviderId() {
        return keyProviderName;
    }

    @Override
    public String getKeyId() {
        return keyProvider.getKeyId();
    }

    @Override
    public DataKey<CryptoMasterKey> generateDataKey(final CryptoAlgorithm algorithm,
                                                    final Map<String, String> encryptionContext) {
        final var dataKeyPair = keyProvider.generateDataPair();
        return new DataKey<>(
            new SecretKeySpec(dataKeyPair.getRawKey(), algorithm.getDataKeyAlgo()),
            dataKeyPair.getEncryptedKey(),
            getKeyId().getBytes(StandardCharsets.UTF_8),
            this);
    }

    @Override
    public DataKey<CryptoMasterKey> encryptDataKey(final CryptoAlgorithm algorithm,
                                                   final Map<String, String> encryptionContext,
                                                   final DataKey<?> dataKey) {
        throw new UnsupportedOperationException("A message carries exactly one encrypted data key");
    }

    @Override
    public DataKey<CryptoMasterKey> decryptDataKey(final CryptoAlgorithm algorithm,
                                                   final Collection<? extends EncryptedDataKey> encryptedDataKeys,
                                                   final Map<String, String> encryptionContext) {
        if (encryptedDataKeys == null || encryptedDataKeys.size() != 1) {
            throw new IllegalArgumentException("Expected exactly one encrypted data key");
        }
        final var encryptedDataKey = encryptedDataKeys.iterator().next();
        if (!keyProviderName.equals(encryptedDataKey.getProviderId())) {
            throw new IllegalArgumentException("Data key was encrypted by key provider ["
                + encryptedDataKey.getProviderId() + "], not [" + keyProviderName + "]");
        }
        final var keyId = new String(encryptedDataKey.getProviderInformation(), StandardCharsets.UTF_8);
        if (!getKeyId().equals(keyId)) {
            throw new IllegalArgumentException("Data key was encrypted with key [" + keyId + "], not [" + getKeyId() + "]");
        }
        return new DataKey<>(
            new SecretKeySpec(keyProvider.decryptKey(encryptedDataKey.getEncryptedDataKey()), algorithm.getDataKeyAlgo()),
            encryptedDataKey.getEncryptedDataKey(),
            encryptedDataKey.getProviderInformation(),
            this);
    }
}
