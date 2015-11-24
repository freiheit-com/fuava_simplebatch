/**
 * Copyright 2015 freiheit.com technologies gmbh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.freiheit.fuava.simplebatch.processor;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * A processor implementation which delegates processing of lists of
 * (successful) values to a function.
 *
 * If persisting of a batch failed, it will be divided into singleton batches
 * and retried.
 *
 * You have to ensure that aborting and retying the function will not lead to
 * illegal states.
 *
 * If your function persists to databases for example, you may need to ensure
 * that your function opens and closes the toplevel transaction and rolls back
 * for <b>all</b> exceptions.
 *
 *
 * @param <Input>
 * @param <Output>
 */
class RetryingProcessor<Input, Output, ProcessorResult> implements Processor<Input, Output, ProcessorResult> {
    private final Function<List<Output>, List<ProcessorResult>> _func;
    private static final Logger LOG = LoggerFactory.getLogger( RetryingProcessor.class );

    /**
     * Creates a new processor that delegates to the given function.
     *
     * Note that you need to ensure, that the input and output lists correspond
     * to each other and that the function supports retrying. For details, see
     * the class documentation.
     *
     * You have to ensure that your input and output lists have the same amount
     * of rows. The processor will assume that each position of input and output
     * corresponds to each other and will associate results accordingly.
     *
     * Note that this function only gets the successfully processed Output
     * values. If you need to persist all, you need to implement the Persistence
     * interface yourself.
     * 
     * @param func
     */
    public RetryingProcessor( final Function<List<Output>, List<ProcessorResult>> func ) {
        _func = func;
    }

    @Override
    public Iterable<Result<Input, ProcessorResult>> process( final Iterable<Result<Input, Output>> inputs ) {

        final List<Result<Input, Output>> inputList = ImmutableList.copyOf( inputs );
        if ( inputList.isEmpty() ) {
            return ImmutableList.of();
        }
        try {
            return doPersist( inputList );
        } catch ( final Throwable t ) {
            if ( inputList.size() == 1 ) {
                final Result<Input, Output> result = inputList.get( 0 );
                return ImmutableList.of( Result.<Input, ProcessorResult> builder( result ).failed( t ) );
            }
            LOG.info( "Caught Exception during processing of batch with " + inputList.size()
                    + " items, will RETRY in single item batches", t );
            final ImmutableList.Builder<Result<Input, ProcessorResult>> retriedResults = ImmutableList.builder();
            for ( final Result<Input, Output> input : inputList ) {
                final Iterable<Result<Input, ProcessorResult>> outputs = process( ImmutableList.of( input ) );
                if ( Iterables.isEmpty( outputs ) ) {
                    throw new IllegalStateException( "processing of singletons must never lead to empty lists here" );
                }
                retriedResults.addAll( outputs );
            }
            return retriedResults.build();
        }
    }

    private Iterable<Result<Input, ProcessorResult>> doPersist( final Iterable<Result<Input, Output>> iterable ) {
        final ImmutableList<Result<Input, Output>> successes = FluentIterable.from( iterable ).filter( Result::isSuccess ).toList();
        final ImmutableList<Result<Input, Output>> fails = FluentIterable.from( iterable ).filter( Result::isFailed ).toList();

        final ImmutableList<Output> outputs = FluentIterable.from( successes ).transform( Result::getOutput ).toList();

        final List<ProcessorResult> persistenceResults = outputs.isEmpty()
            ? ImmutableList.of()
            : this._func.apply( outputs );

        if ( persistenceResults.size() != outputs.size() || persistenceResults.size() != successes.size() ) {
            throw new IllegalStateException( "persistence results of unexpected size" );
        }
        final ImmutableList.Builder<Result<Input, ProcessorResult>> b = ImmutableList.builder();

        for ( int i = 0; i < outputs.size(); i++ ) {
            final Result<Input, Output> processingResult = successes.get( i );
            final ProcessorResult persistenceResult = persistenceResults.get( i );
            b.add( Result.<Input, ProcessorResult> builder( processingResult ).withOutput( persistenceResult ).success() );
        }

        for ( final Result<Input, Output> failed : fails ) {
            b.add( Result.<Input, ProcessorResult> builder( failed ).failed() );
        }

        return b.build();
    }
}