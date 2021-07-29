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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.opensearch.common.settings.SecureSettings;
import org.opensearch.common.settings.SecureString;

public class DummySecureSettings implements SecureSettings {

    private Map<String, byte[]> files = new HashMap<>();

    private Map<String, String> values = new HashMap<>();

    @Override
    public boolean isLoaded() {
        return true;
    }

    @Override
    public Set<String> getSettingNames() {
        return Stream.concat(files.keySet().stream(), values.keySet().stream()).collect(Collectors.toSet());
    }

    public DummySecureSettings setString(final String name, final String value) {
        values.put(name, value);
        return this;
    }

    public DummySecureSettings setFile(final String setting,
                                       final InputStream in) throws IOException {
        try (final var bytesStream = new ByteArrayOutputStream()) {
            in.transferTo(bytesStream);
            bytesStream.flush();
            files.put(setting, bytesStream.toByteArray());
            return this;
        }
    }

    @Override
    public SecureString getString(final String setting) throws GeneralSecurityException {
        return new SecureString(values.get(setting).toCharArray());
    }

    @Override
    public InputStream getFile(final String setting) throws GeneralSecurityException {
        return new ByteArrayInputStream(files.get(setting));
    }

    @Override
    public byte[] getSHA256Digest(final String setting) throws GeneralSecurityException {
        return new byte[0];
    }

    @Override
    public void close() throws IOException {
    }
}
