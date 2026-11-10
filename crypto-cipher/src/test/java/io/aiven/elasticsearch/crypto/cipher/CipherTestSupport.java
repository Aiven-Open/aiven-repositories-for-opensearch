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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Security;
import java.util.Arrays;
import java.util.Random;

import org.opensearch.common.crypto.EncryptedHeaderContentSupplier;
import org.opensearch.common.io.InputStreamContainer;

import io.aiven.elasticsearch.crypto.provider.RsaMasterKeyProvider;

import com.amazonaws.encryptionsdk.AwsCrypto;
import com.amazonaws.encryptionsdk.CommitmentPolicy;
import com.amazonaws.encryptionsdk.jce.JceMasterKey;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.junit.jupiter.api.BeforeAll;

/**
 * Runs with {@code BouncyCastleFipsProvider} as the first JCE provider when the system property
 * {@value #BCFIPS_PROPERTY} is {@code true}, which is how the {@code testWithBcFips} task runs every test.
 */
abstract class CipherTestSupport {

    static final String BCFIPS_PROPERTY = "aiven.crypto.test.bcfips";

    static final String KEY_PROVIDER_NAME = "default";

    static final int SMALL_FRAME_LENGTH = 64;

    static final Random RANDOM = new Random(42);

    static KeyPair keyPair;

    static KeyPair otherKeyPair;

    @BeforeAll
    static void setUpProvidersAndKeys() throws Exception {
        if (Boolean.getBoolean(BCFIPS_PROPERTY) && Security.getProvider(BouncyCastleFipsProvider.PROVIDER_NAME) == null) {
            Security.insertProviderAt(new BouncyCastleFipsProvider(), 1);
        }
        if (keyPair == null) {
            final var generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            keyPair = generator.generateKeyPair();
            otherKeyPair = generator.generateKeyPair();
        }
    }

    static RsaMasterKeyProvider keyProvider() {
        return new RsaMasterKeyProvider(keyPair);
    }

    static AwsMessageFormatCryptoHandler handler(final int frameLength) {
        return handler(keyPair, frameLength);
    }

    static AwsMessageFormatCryptoHandler handler(final KeyPair keyPair, final int frameLength) {
        return new AwsMessageFormatCryptoHandler(
            AwsMessageFormatCryptoPlugin.materialsManager(new RsaMasterKeyProvider(keyPair), KEY_PROVIDER_NAME),
            frameLength,
            () -> { });
    }

    /** The AWS Encryption SDK raw RSA keyring an external tool uses to read our blobs. */
    static JceMasterKey jceMasterKey() {
        return JceMasterKey.getInstance(
            keyPair.getPublic(),
            keyPair.getPrivate(),
            KEY_PROVIDER_NAME,
            keyProvider().getKeyId(),
            "RSA/ECB/OAEPWithSHA-256AndMGF1Padding");
    }

    static AwsCrypto awsCrypto(final int frameLength) {
        return AwsCrypto.builder()
            .withEncryptionAlgorithm(AwsMessageFormatCryptoHandler.ALGORITHM)
            .withCommitmentPolicy(CommitmentPolicy.RequireEncryptRequireDecrypt)
            .withEncryptionFrameSize(frameLength)
            .build();
    }

    static byte[] randomBytes(final int length) {
        final var bytes = new byte[length];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    static byte[] encrypt(final AwsMessageFormatCryptoHandler handler,
                          final MessageHeader header,
                          final byte[] plaintext) throws IOException {
        final var container = handler.createEncryptingStream(
            header, new InputStreamContainer(new ByteArrayInputStream(plaintext), plaintext.length, 0));
        try (final var in = container.getInputStream()) {
            final var ciphertext = in.readAllBytes();
            if (ciphertext.length != container.getContentLength()) {
                throw new AssertionError("Stream length " + ciphertext.length + " != declared " + container.getContentLength());
            }
            return ciphertext;
        }
    }

    static byte[] encrypt(final AwsMessageFormatCryptoHandler handler, final byte[] plaintext) throws IOException {
        return encrypt(handler, handler.initEncryptionMetadata(), plaintext);
    }

    static byte[] decrypt(final AwsMessageFormatCryptoHandler handler, final byte[] ciphertext) throws IOException {
        try (final var in = handler.createDecryptingStream(new ByteArrayInputStream(ciphertext))) {
            return in.readAllBytes();
        }
    }

    /** Behaves like core's {@code EncryptedBlobContainer} supplier: zero padding past the end of the blob. */
    static EncryptedHeaderContentSupplier headerSupplier(final byte[] blob) {
        return (start, end) -> {
            final var buffer = new byte[(int) (end - start + 1)];
            System.arraycopy(blob, (int) start, buffer, 0, (int) Math.min(buffer.length, blob.length - start));
            return buffer;
        };
    }

    static byte[] slice(final byte[] bytes, final long from, final long toInclusive) {
        return Arrays.copyOfRange(bytes, (int) from, (int) Math.min(toInclusive + 1, bytes.length));
    }
}
