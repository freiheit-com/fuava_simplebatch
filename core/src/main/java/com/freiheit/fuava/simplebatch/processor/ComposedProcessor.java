package com.freiheit.fuava.simplebatch.processor;

import com.freiheit.fuava.simplebatch.result.Result;

final class ComposedProcessor<A, B, C, D> implements Processor<A, B, D> {
    private final Processor<A, B, C> f;
    private final Processor<A, C, D> g;

    ComposedProcessor( final Processor<A, C, D> g, final Processor<A, B, C> f ) {
        this.g = g;
        this.f = f;
    }

    @Override
    public Iterable<Result<A, D>> process( final Iterable<Result<A, B>> toPersist ) {
        final Iterable<Result<A, C>> fResults = this.f.process( toPersist );
        final Iterable<Result<A, D>> gResults = this.g.process( fResults );
        return gResults;
    }

}