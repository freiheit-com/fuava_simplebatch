package com.freiheit.fuava.simplebatch.processor;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

final class ParallelProcessor<A, B, D> implements Processor<A, B, D> {
    private final Processor<A, B, D> f;
    private final Processor<A, B, D> g;

    ParallelProcessor( final Processor<A, B, D> g, final Processor<A, B, D> f ) {
        this.g = g;
        this.f = f;
    }

    @Override
    public Iterable<Result<A, D>> process( final Iterable<Result<A, B>> toPersist ) {
        Builder<Result<A, D>> resultsBuilder = new ImmutableList.Builder<Result<A, D>>();

        resultsBuilder.addAll( this.f.process( toPersist ) );
        resultsBuilder.addAll( this.g.process( toPersist ) );
        
        return resultsBuilder.build();
    }

}