package com.freiheit.fuava.simplebatch.result;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;

public interface ProcessingResultListener<Input, Output> {

    default void onBeforeRun( final String description ) {
    }

    default void onAfterRun() {
    }

    default void onFetchResult( final Result<FetchedItem<Input>, Input> result ) {
    }

    default void onFetchResults( final Iterable<Result<FetchedItem<Input>, Input>> result ) {
        for ( final Result<FetchedItem<Input>, Input> r : result ) {
            onFetchResult( r );
        }
    }

    default void onProcessingResult( final Result<FetchedItem<Input>, Output> result ) {

    }

    default void onProcessingResults( final Iterable<? extends Result<FetchedItem<Input>, Output>> results ) {
        for ( final Result<FetchedItem<Input>, Output> r : results ) {
            onProcessingResult( r );
        }
    }

}
