package com.freiheit.fuava.simplebatch.processor;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.collect.ImmutableList;

public abstract class AbstractSingleItemProcessor<Input, Output, P> implements Processor<Input, Output, P> {

    @Override
    public final Iterable<Result<Input, P>> process( final Iterable<Result<Input, Output>> iterable ) {
        final ImmutableList.Builder<Result<Input, P>> b = ImmutableList.builder();
        for ( final Result<Input, Output> input : iterable ) {
            b.add( processItem( input ) );
        }
        return b.build();
    }

    public abstract Result<Input, P> processItem( Result<Input, Output> input );
}