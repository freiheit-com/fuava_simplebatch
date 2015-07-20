package com.freiheit.fuava.simplebatch.result;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;

public class DelegatingProcessingResultListener<Input, Output> implements ProcessingResultListener<Input, Output> {
    static final Logger LOG = LoggerFactory.getLogger( DelegatingProcessingResultListener.class );

    private final Iterable<ProcessingResultListener<Input, Output>> listeners;

    private boolean hasDelegationFailures;

    public DelegatingProcessingResultListener( final Iterable<ProcessingResultListener<Input, Output>> listeners ) {
        this.listeners = listeners;
    }

    public boolean hasDelegationFailures() {
        return hasDelegationFailures;
    }

    protected void onListenerFailure( final ProcessingResultListener<Input, Output> l, final String fktName, final Throwable t ) {
        hasDelegationFailures = true;
        LOG.error( "Failed to call Listener " + l + " for " + fktName, t );
    }

    @Override
    public void onBeforeRun( final String description ) {
        for ( final ProcessingResultListener<Input, Output> l : listeners ) {
            try {
                l.onBeforeRun( description );
            } catch ( final Throwable t ) {
                onListenerFailure( l, "onBeforeRun", t );
            }
        }
    }

    @Override
    public void onAfterRun() {
        for ( final ProcessingResultListener<Input, Output> l : listeners ) {
            try {
                l.onAfterRun();
            } catch ( final Throwable t ) {
                onListenerFailure( l, "onAfterRun", t );
            }
        }
    }

    @Override
    public void onFetchResult( final Result<FetchedItem<Input>, Input> result ) {
        for ( final ProcessingResultListener<Input, Output> l : listeners ) {
            try {
                l.onFetchResult( result );
            } catch ( final Throwable t ) {
                onListenerFailure( l, "onFetchResult", t );
            }
        }
    }

    @Override
    public void onFetchResults( final Iterable<Result<FetchedItem<Input>, Input>> results ) {
        for ( final ProcessingResultListener<Input, Output> l : listeners ) {
            try {
                l.onFetchResults( results );
            } catch ( final Throwable t ) {
                onListenerFailure( l, "onFetchResults", t );
            }
        }
    }

    @Override
    public void onProcessingResult( final Result<FetchedItem<Input>, Output> result ) {
        for ( final ProcessingResultListener<Input, Output> l : listeners ) {
            try {
                l.onProcessingResult( result );
            } catch ( final Throwable t ) {
                onListenerFailure( l, "onPersistResult", t );
            }
        }
    }

    @Override
    public void onProcessingResults( final Iterable<? extends Result<FetchedItem<Input>, Output>> results ) {
        for ( final ProcessingResultListener<Input, Output> l : listeners ) {
            try {
                l.onProcessingResults( results );
            } catch ( final Throwable t ) {
                onListenerFailure( l, "onPersistResults", t );
            }
        }
    }

}
