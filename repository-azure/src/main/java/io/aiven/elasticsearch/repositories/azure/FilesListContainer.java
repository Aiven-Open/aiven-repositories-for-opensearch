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

package io.aiven.elasticsearch.repositories.azure;

import java.util.Collections;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.azure.core.http.HttpPipeline;
import com.azure.core.http.rest.PagedFlux;
import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.PagedResponse;
import com.azure.core.http.rest.PagedResponseBase;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobServiceVersion;
import com.azure.storage.blob.implementation.AzureBlobStorageBuilder;
import com.azure.storage.blob.implementation.AzureBlobStorageImpl;
import com.azure.storage.blob.implementation.models.ContainerListBlobHierarchySegmentHeaders;
import com.azure.storage.blob.implementation.util.ModelHelper;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.implementation.StorageImplUtils;
import reactor.core.publisher.Mono;

class FilesListContainer {

    private final AzureBlobStorageImpl azureBlobStorage;

    FilesListContainer(final HttpPipeline httpPipeline, final String url) {
        azureBlobStorage =
                new AzureBlobStorageBuilder()
                        .pipeline(httpPipeline)
                        .url(url)
                        .version(BlobServiceVersion.getLatest().getVersion())
                        .build();
    }

    /** due to the diff version of jackson. Mapping oif XML tag <NextMarker/> returns empty value instead of null,
     * while in the new version this behave has been changed,
     * as a result recursive call which checks only `null` value for the continuation token can't properly stop.
     * In case ES will bump up jackson-core lib to version 2.11.x
     * we can exclude this solution and use default client behave **/
    public PagedIterable<BlobItem> list(final String path) {
        final var options = new ListBlobsOptions().setPrefix(path);
        final Function<String, Mono<PagedResponse<BlobItem>>> retriever = marker ->
                StorageImplUtils.applyOptionalTimeout(
                        this.azureBlobStorage.containers().listBlobHierarchySegmentWithRestResponseAsync(null, null,
                        options.getPrefix(), marker, options.getMaxResultsPerPage(), null, null, null,
                        Context.NONE), null)
                        .map(response -> {
                            final var segment = response.getValue().getSegment();
                            final var blobItems = Objects.isNull(segment)
                                    ? Collections.<BlobItem>emptyList()
                                    : Stream.concat(
                                            segment.getBlobItems().stream().map(ModelHelper::populateBlobItem),
                                            segment.getBlobPrefixes().stream().map(blobPrefix ->
                                                    new BlobItem().setName(blobPrefix.getName()).setIsPrefix(true)))
                                            .collect(Collectors.toList());
                            final var nextMarker =
                                    (Objects.nonNull(response.getValue().getNextMarker())
                                            && response.getValue().getNextMarker().isEmpty())
                                            ? null
                                            : response.getValue().getNextMarker();
                            return new PagedResponseBase<ContainerListBlobHierarchySegmentHeaders, BlobItem>(
                                    response.getRequest(),
                                    response.getStatusCode(),
                                    response.getHeaders(),
                                    blobItems,
                                    nextMarker,
                                    response.getDeserializedHeaders()
                            );
                        });

        return new PagedIterable<>(new PagedFlux<>(() -> retriever.apply(null)));
    }


}
