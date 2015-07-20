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

    public LazyPageFetchingIterable(
            final PageFetcher<T> fetcher, final int initialFrom,
            final int pageSize, final PageFetchingSettings<T> settings ) {
        this.fetcher = fetcher;
        this.from = initialFrom;
        this.pageSize = pageSize;
        this.settings = settings;
        next = advance();
    }

    @Override
    public boolean hasNext() {
        return settings.hasNext( from, pageSize, next );
    }

    @Override
    public Result<PagingInput, T> next() {
        final Result<PagingInput, T> v = next;
        next = advance();
        return v;
    }

    private Result<PagingInput, T> advance() {
        final Result<PagingInput, T> v = fetcher.fetch( from, pageSize );
        from += pageSize;
        return v;
    }
}