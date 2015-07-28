package com.freiheit.fuava.simplebatch.fetch;

import java.util.Iterator;

import com.freiheit.fuava.simplebatch.fetch.PageFetcher.PagingInput;
import com.freiheit.fuava.simplebatch.result.Result;

public class LazyPageFetchingIterable<T> implements Iterator<Result<PagingInput, T>> {
    private final PageFetcher<T> fetcher;

    private final int pageSize;
    private final PageFetchingSettings<T> settings;

    private int from;

    private Result<PagingInput, T> next;
    private boolean hasNext;

    public LazyPageFetchingIterable(
            final PageFetcher<T> fetcher, final int initialFrom,
            final int pageSize, final PageFetchingSettings<T> settings ) {
        this.fetcher = fetcher;
        this.from = initialFrom;
        this.pageSize = pageSize;
        this.settings = settings;
        advance();
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public Result<PagingInput, T> next() {
        final Result<PagingInput, T> v = next;
        // let the settings control wether or not we expect to see more results and thus need another page call
        if ( !settings.hasNext( from, pageSize, next ) ) {
            hasNext = false;
            next = null;
        } else {
            advance();
        }
        return v;
    }

    private void advance() {
        final Result<PagingInput, T> v = fetcher.fetch( from, pageSize );
        from += pageSize;
        hasNext = v != null;
        next = v;
    }
}