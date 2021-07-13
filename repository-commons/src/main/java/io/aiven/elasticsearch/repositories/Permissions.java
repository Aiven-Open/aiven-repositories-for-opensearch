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
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.opensearch.SpecialPermission;
import org.opensearch.common.CheckedRunnable;

public final class Permissions {

    private Permissions() {
    }

    public static <T> T doPrivileged(final PrivilegedExceptionAction<T> privilegedAction) throws IOException {
        SpecialPermission.check();
        try {
            return AccessController.doPrivileged(privilegedAction);
        } catch (final PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }

    public static void doPrivileged(final CheckedRunnable<IOException> checkedRunnable) throws IOException {
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                checkedRunnable.run();
                return null;
            });
        } catch (final PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }

}
