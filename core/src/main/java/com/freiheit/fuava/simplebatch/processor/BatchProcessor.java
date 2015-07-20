package com.freiheit.fuava.simplebatch.processor;

import java.util.List;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

final class BatchProcessor<Input, Output, P> implements Processor<Input, Output, BatchProcessorResult<P>> {
    private final Processor<List<Input>, List<Output>, P> delegee;

    public BatchProcessor( final Processor<List<Input>, List<Output>, P> delegee ) {
        this.delegee = delegee;
    }

    @Override
    public Iterable<Result<Input, BatchProcessorResult<P>>> process( final Iterable<Result<Input, Output>> iterable ) {
        final List<Result<Input, Output>> success = FluentIterable.from( iterable ).filter( Result::isSuccess ).toList();
        final List<Result<Input, Output>> fails = FluentIterable.from( iterable ).filter( Result::isFailed ).toList();

        final ImmutableList.Builder<Result<Input, BatchProcessorResult<P>>> resultBuilder = ImmutableList.builder();
        if ( !success.isEmpty() ) {
            final List<Input> successInputs = FluentIterable.from( success ).transform( Result::getInput ).toList();
            final List<Output> successOutputs = FluentIterable.from( success ).transform( Result::getOutput ).toList();

            final Iterable<Result<List<Input>, P>> batchResults =
                    this.delegee.process( ImmutableList.of( Result.<List<Input>, List<Output>> success( successInputs,
                            successOutputs ) ) );

            for ( final Result<List<Input>, P> r : batchResults ) {
                final List<Input> inputs = r.getInput();
                final P batchOutput = r.getOutput();
                int row = 0;
                final int total = inputs.size();
                for ( final Input input : inputs ) {
                    resultBuilder.add( Result.success( input, new BatchProcessorResult<P>( batchOutput, row, total ) ) );
                    row++;
                }
            }
        }

        for ( final Result<Input, Output> fail : fails ) {
            resultBuilder.add( Result.<Input, BatchProcessorResult<P>> builder( fail ).failed() );
        }
        return resultBuilder.build();
    }
}