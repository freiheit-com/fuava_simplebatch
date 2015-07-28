package com.freiheit.fuava.simplebatch.fetch;

import com.freiheit.fuava.simplebatch.fetch.PageFetcher.PagingInput;
import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.collect.Iterables;

public interface PageFetchingSettings<T> {
    default boolean hasNext( final int from, final int amount, final Result<PagingInput, T> lastValue ) {
        if ( lastValue == null || !lastValue.isSuccess() ) {
            return false;
        }
        final Object output = lastValue.getOutput();
        if ( output instanceof Iterable ) {
            return Iterables.size( (Iterable) output ) >= amount;
        }
        throw new UnsupportedOperationException( "cannot calculate paging for " + output
                + " - please provide your own implementation" );

    }
}