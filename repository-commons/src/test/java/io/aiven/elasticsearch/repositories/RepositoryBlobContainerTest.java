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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStoreException;
import org.opensearch.common.collect.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static io.aiven.elasticsearch.repositories.RepositoryStorageIOProvider.StorageIO;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RepositoryBlobContainerTest {

    static final BlobPath DEFAULT_PATH = BlobPath.cleanPath().add("/some/path");

    @Mock
    StorageIO mockedStorageIO;

    RepositoryBlobContainer repositoryBlobContainer;

    @BeforeEach
    void setUp() {
        repositoryBlobContainer = new RepositoryBlobContainer(DEFAULT_PATH, mockedStorageIO);
    }

    @Test
    void blobExists() throws IOException {
        repositoryBlobContainer.blobExists("some_blob");
        verify(mockedStorageIO).exists(DEFAULT_PATH.buildAsString() + "some_blob");
    }

    @Test
    void blobExistsThrowsBlobStoreException() throws IOException {
        when(mockedStorageIO.exists("some_blob")).thenThrow(IllegalArgumentException.class);
        assertThrows(
                BlobStoreException.class, () -> repositoryBlobContainer.blobExists("some_blob"));
    }

    @Test
    void readBlob() throws IOException {
        repositoryBlobContainer.readBlob("some_blob");
        verify(mockedStorageIO).read(DEFAULT_PATH.buildAsString() + "some_blob");
    }

    @Test
    void readBlobWithPosition() throws IOException {
        assertThrows(
                UnsupportedOperationException.class, () -> repositoryBlobContainer
                        .readBlob("some_blob", 0, 100));
    }

    @Test
    void writeBlob() throws IOException {
        final var nullStream = InputStream.nullInputStream();

        repositoryBlobContainer
                .writeBlob(
                        "some_blob",
                        nullStream,
                        100L,
                        false);

        verify(mockedStorageIO)
                .write(
                        DEFAULT_PATH.buildAsString() + "some_blob",
                        nullStream,
                        100L,
                        false);
    }

    @Test
    void writeBlobAtomic() throws IOException {
        final var nullStream = InputStream.nullInputStream();

        repositoryBlobContainer
                .writeBlobAtomic(
                        "some_blob",
                        nullStream,
                        100L,
                        false);

        verify(mockedStorageIO)
                .write(
                        DEFAULT_PATH.buildAsString() + "some_blob",
                        nullStream,
                        100L,
                        false);
    }

    @Test
    void delete() throws IOException {
        when(mockedStorageIO.deleteDirectories(DEFAULT_PATH.buildAsString()))
                .thenReturn(Tuple.tuple(10, 100L));

        final var deleteResult = repositoryBlobContainer.delete();

        assertEquals(10, deleteResult.blobsDeleted());
        assertEquals(100L, deleteResult.bytesDeleted());

        verify(mockedStorageIO).deleteDirectories(DEFAULT_PATH.buildAsString());

    }

    @Test
    void deleteBlobsIgnoringIfNotExists() throws IOException {

        repositoryBlobContainer.deleteBlobsIgnoringIfNotExists(List.of("some_blob"));

        verify(mockedStorageIO).deleteFiles(eq(List.of(DEFAULT_PATH.buildAsString() + "some_blob")), eq(true));

    }

    @Test
    void children() throws IOException {

        when(mockedStorageIO.listDirectories(DEFAULT_PATH.buildAsString()))
                .thenReturn(List.of("some_dir1", "some_dir2"));

        final var containers = repositoryBlobContainer.children();

        assertEquals(2, containers.size());
        assertEquals(Set.of("some_dir1", "some_dir2"), containers.keySet());
        assertEquals(
                DEFAULT_PATH.buildAsString() + "some_dir1/",
                containers.get("some_dir1").path().buildAsString()
        );
        assertEquals(
                DEFAULT_PATH.buildAsString() + "some_dir2/",
                containers.get("some_dir2").path().buildAsString()
        );

        verify(mockedStorageIO).listDirectories(DEFAULT_PATH.buildAsString());

    }

    @Test
    void listBlobs() throws IOException {

        when(mockedStorageIO.listFiles(DEFAULT_PATH.buildAsString(), ""))
                .thenReturn(Map.of("some_file1", 100L, "some_file2", 200L));

        final var blobs = repositoryBlobContainer.listBlobs();

        assertEquals(2, blobs.size());
        assertEquals(Set.of("some_file1", "some_file2"), blobs.keySet());
        assertEquals("some_file1", blobs.get("some_file1").name());
        assertEquals(100L, blobs.get("some_file1").length());
        assertEquals("some_file2", blobs.get("some_file2").name());
        assertEquals(200L, blobs.get("some_file2").length());

        verify(mockedStorageIO).listFiles(DEFAULT_PATH.buildAsString(), "");

    }

    @Test
    void listBlobsByPrefix() throws IOException {
        when(mockedStorageIO.listFiles(DEFAULT_PATH.buildAsString(), "some_prefix"))
                .thenReturn(Map.of("some_file1", 100L, "some_file2", 200L));

        final var blobs =
                repositoryBlobContainer.listBlobsByPrefix("some_prefix");

        assertEquals(2, blobs.size());
        assertEquals(Set.of("some_file1", "some_file2"), blobs.keySet());
        assertEquals("some_file1", blobs.get("some_file1").name());
        assertEquals(100L, blobs.get("some_file1").length());
        assertEquals("some_file2", blobs.get("some_file2").name());
        assertEquals(200L, blobs.get("some_file2").length());

        verify(mockedStorageIO).listFiles(DEFAULT_PATH.buildAsString(), "some_prefix");

    }

}
