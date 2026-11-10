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

import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.util.List;
import java.util.Map;

import com.amazonaws.encryptionsdk.CommitmentPolicy;
import com.amazonaws.encryptionsdk.model.DecryptionMaterialsRequest;
import com.amazonaws.encryptionsdk.model.EncryptionMaterialsRequest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MaterialsManagerTest extends CipherTestSupport {

    @Test
    void bouncyCastleIsNotBundled() {
        assertThrows(ClassNotFoundException.class, () -> Class.forName("org.bouncycastle.jce.provider.BouncyCastleProvider"));
        if (Boolean.getBoolean(BCFIPS_PROPERTY)) {
            assertNotNull(Security.getProvider("BCFIPS"));
            assertEquals("BCFIPS", Security.getProviders()[0].getName());
        } else {
            assertNull(Security.getProvider("BCFIPS"));
        }
    }

    @Test
    void cachesDataKeyForEncryptionAndUnwrapsItForDecryption() {
        final var materialsManager = AwsMessageFormatCryptoPlugin.materialsManager(keyProvider(), KEY_PROVIDER_NAME);
        final var request = EncryptionMaterialsRequest.newBuilder()
            .setContext(Map.of())
            .setRequestedAlgorithm(AwsMessageFormatCryptoHandler.ALGORITHM)
            .setCommitmentPolicy(CommitmentPolicy.RequireEncryptRequireDecrypt)
            .setPlaintextSize(0)
            .build();

        final var first = materialsManager.getMaterialsForEncrypt(request);
        final var second = materialsManager.getMaterialsForEncrypt(request);

        assertEquals(AwsMessageFormatCryptoHandler.ALGORITHM, first.getAlgorithm());
        assertArrayEquals(first.getCleartextDataKey().getEncoded(), second.getCleartextDataKey().getEncoded());
        final var encryptedDataKey = first.getEncryptedDataKeys().get(0);
        assertArrayEquals(encryptedDataKey.getEncryptedDataKey(), second.getEncryptedDataKeys().get(0).getEncryptedDataKey());
        assertEquals(KEY_PROVIDER_NAME, encryptedDataKey.getProviderId());
        assertEquals(keyProvider().getKeyId(),
            new String(encryptedDataKey.getProviderInformation(), StandardCharsets.UTF_8));

        final var decrypted = materialsManager.decryptMaterials(DecryptionMaterialsRequest.newBuilder()
            .setAlgorithm(AwsMessageFormatCryptoHandler.ALGORITHM)
            .setEncryptionContext(Map.of())
            .setEncryptedDataKeys(List.of(encryptedDataKey))
            .build());
        assertArrayEquals(first.getCleartextDataKey().getEncoded(), decrypted.getDataKey().getKey().getEncoded());
    }
}
