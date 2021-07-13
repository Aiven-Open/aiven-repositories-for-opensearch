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
import java.io.UncheckedIOException;
import java.util.List;

import io.aiven.elasticsearch.repositories.AbstractRepositoryPlugin;
import io.aiven.elasticsearch.repositories.Permissions;

import com.amazonaws.services.s3.AmazonS3;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

public class S3RepositoryPlugin extends AbstractRepositoryPlugin<AmazonS3> {

    public static final String REPOSITORY_TYPE = "aiven-s3";

    public S3RepositoryPlugin(final Settings settings) {
        super(REPOSITORY_TYPE, settings, new S3SettingsProvider());
    }

    @Override
    public List<Setting<?>> getSettings() {
        try {
            //due to the load of constants for AWS SDK use check permissions here
            return Permissions.doPrivileged(() ->
                    List.of(
                            S3StorageSettings.PUBLIC_KEY_FILE,
                            S3StorageSettings.PRIVATE_KEY_FILE,
                            S3StorageSettings.AWS_SECRET_ACCESS_KEY,
                            S3StorageSettings.AWS_ACCESS_KEY_ID,
                            S3StorageSettings.ENDPOINT,
                            S3StorageSettings.MAX_RETRIES,
                            S3StorageSettings.READ_TIMEOUT,
                            S3StorageSettings.USE_THROTTLE_RETRIES
                    ));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
