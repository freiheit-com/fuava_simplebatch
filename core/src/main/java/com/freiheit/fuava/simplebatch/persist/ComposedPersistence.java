package com.freiheit.fuava.simplebatch.persist;

import com.freiheit.fuava.simplebatch.result.Result;

final class ComposedPersistence<A, B, C, D> implements Persistence<A, B, D> {
    private final Persistence<A, B, C> f;
    private final Persistence<A, C, D> g;

    ComposedPersistence( final Persistence<A, C, D> g, final Persistence<A, B, C> f ) {
        this.g = g;
        this.f = f;
    }

    @Override
    public Iterable<Result<A, D>> persist( final Iterable<Result<A, B>> toPersist ) {
        final Iterable<Result<A, C>> fResults = this.f.persist( toPersist );
        final Iterable<Result<A, D>> gResults = this.g.persist( fResults );
        return gResults;
    }

}