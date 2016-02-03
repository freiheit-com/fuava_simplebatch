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

public class ResultStatistics {

    public static final class Builder<OriginalInput, Output> implements ProcessingResultListener<OriginalInput, Output> {

        private final Counts.Builder fetch = Counts.builder();
        private final Counts.Builder processing = Counts.builder();
        private boolean hasListenerDelegationFailures = false;

        public ResultStatistics build() {
            return new ResultStatistics(
                    fetch.build(),
                    processing.build(),
                    hasListenerDelegationFailures );
        }

        public void setListenerDelegationFailures( final boolean b ) {
            hasListenerDelegationFailures = b;
        }

        @Override
        public void onFetchResult( final Result<FetchedItem<OriginalInput>, OriginalInput> result ) {
            fetch.add( result );
        }

        @Override
        public void onProcessingResult( final Result<FetchedItem<OriginalInput>, Output> result ) {
            processing.add( result );
        }

    }

    private final Counts fetch;
    private final Counts processing;
    private final boolean hasListenerDelegationFailures;

    public ResultStatistics( final Counts fetch, final Counts persist, final boolean hasListenerDelegationFailures ) {
        this.fetch = fetch;
        this.processing = persist;
        this.hasListenerDelegationFailures = hasListenerDelegationFailures;
    }

    public Counts getFetchCounts() {
        return fetch;
    }

    public Counts getProcessingCounts() {
        return processing;
    }

    private static boolean allFailed( final Counts counts ) {
        return counts.getError() != 0 && counts.getSuccess() == 0;
    }

    private static boolean allSuccess( final Counts counts ) {
        return counts.getError() == 0;
    }

    public boolean isAllFailed() {
        return allFailed( fetch )
                || allFailed( processing );

    }

    public boolean isAllSuccess() {
        return allSuccess( fetch )
                && allSuccess( processing )
                && !hasListenerDelegationFailures();
    }

    public boolean hasListenerDelegationFailures() {
        return hasListenerDelegationFailures;
    }

    public static final <OriginalInput, Output> Builder<OriginalInput, Output> builder() {
        return new Builder<OriginalInput, Output>();
    }
}
