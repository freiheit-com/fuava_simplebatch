/**
 * Copyright 2015 freiheit.com technologies gmbh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.freiheit.fuava.simplebatch.result;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;

public interface ProcessingResultListener<OriginalInput, Output> {

    default void onBeforeRun( final String description ) {
    }

    default void onAfterRun() {
    }

    default void onFetchResult( final Result<FetchedItem<OriginalInput>, OriginalInput> result ) {
    }

    default void onFetchResults( final Iterable<Result<FetchedItem<OriginalInput>, OriginalInput>> result ) {
        for ( final Result<FetchedItem<OriginalInput>, OriginalInput> r : result ) {
            onFetchResult( r );
        }
    }

    default void onProcessingResult( final Result<FetchedItem<OriginalInput>, Output> result ) {

    }

    default void onProcessingResults( final Iterable<? extends Result<FetchedItem<OriginalInput>, Output>> results ) {
        for ( final Result<FetchedItem<OriginalInput>, Output> r : results ) {
            onProcessingResult( r );
        }
    }

}
