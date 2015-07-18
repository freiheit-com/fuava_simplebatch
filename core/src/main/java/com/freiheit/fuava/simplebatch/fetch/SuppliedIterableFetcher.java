package com.freiheit.fuava.simplebatch.fetch;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;

public final class SuppliedIterableFetcher<T> implements Fetcher<T>{
    private final Supplier<Iterable<T>> supplier;

    public SuppliedIterableFetcher(Supplier<Iterable<T>> supplier) {
        this.supplier = supplier;
    }

    @Override
    public Iterable<Result<T, T>> fetchAll() {
        try {
            return new IterableFetcherWrapper<T>(this.supplier.get());
        } catch (Throwable t) {
            return ImmutableList.of(Result.failed(null, t));
        }
    }

}