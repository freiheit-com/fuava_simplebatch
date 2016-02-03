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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;

public class DelegatingProcessingResultListener<OriginalInput, Output> implements ProcessingResultListener<OriginalInput, Output> {
    static final Logger LOG = LoggerFactory.getLogger( DelegatingProcessingResultListener.class );

    private final Iterable<ProcessingResultListener<OriginalInput, Output>> listeners;

    private boolean hasDelegationFailures;

    public DelegatingProcessingResultListener( final Iterable<ProcessingResultListener<OriginalInput, Output>> listeners ) {
        this.listeners = listeners;
    }

    public boolean hasDelegationFailures() {
        return hasDelegationFailures;
    }

    protected void onListenerFailure( final ProcessingResultListener<OriginalInput, Output> l, final String fktName, final Throwable t ) {
        hasDelegationFailures = true;
        LOG.error( "Failed to call Listener " + l + " for " + fktName, t );
    }

    @Override
    public void onBeforeRun( final String description ) {
        for ( final ProcessingResultListener<OriginalInput, Output> l : listeners ) {
            try {
                l.onBeforeRun( description );
            } catch ( final Throwable t ) {
                onListenerFailure( l, "onBeforeRun", t );
            }
        }
    }

    @Override
    public void onAfterRun() {
        for ( final ProcessingResultListener<OriginalInput, Output> l : listeners ) {
            try {
                l.onAfterRun();
            } catch ( final Throwable t ) {
                onListenerFailure( l, "onAfterRun", t );
            }
        }
    }

    @Override
    public void onFetchResult( final Result<FetchedItem<OriginalInput>, OriginalInput> result ) {
        for ( final ProcessingResultListener<OriginalInput, Output> l : listeners ) {
            try {
                l.onFetchResult( result );
            } catch ( final Throwable t ) {
                onListenerFailure( l, "onFetchResult", t );
            }
        }
    }

    @Override
    public void onFetchResults( final Iterable<Result<FetchedItem<OriginalInput>, OriginalInput>> results ) {
        for ( final ProcessingResultListener<OriginalInput, Output> l : listeners ) {
            try {
                l.onFetchResults( results );
            } catch ( final Throwable t ) {
                onListenerFailure( l, "onFetchResults", t );
            }
        }
    }

    @Override
    public void onProcessingResult( final Result<FetchedItem<OriginalInput>, Output> result ) {
        for ( final ProcessingResultListener<OriginalInput, Output> l : listeners ) {
            try {
                l.onProcessingResult( result );
            } catch ( final Throwable t ) {
                onListenerFailure( l, "onPersistResult", t );
            }
        }
    }

    @Override
    public void onProcessingResults( final Iterable<? extends Result<FetchedItem<OriginalInput>, Output>> results ) {
        for ( final ProcessingResultListener<OriginalInput, Output> l : listeners ) {
            try {
                l.onProcessingResults( results );
            } catch ( final Throwable t ) {
                onListenerFailure( l, "onPersistResults", t );
            }
        }
    }

}
