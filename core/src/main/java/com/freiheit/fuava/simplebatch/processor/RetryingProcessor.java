/*
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

import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.util.IterableUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
 * @param <OriginalItem>
 * @param <Input>
 */
public abstract class RetryingProcessor<OriginalItem, Input, Output>
        implements Processor<OriginalItem, Input, Output> {
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
     */
    public RetryingProcessor() {
    }

    @Override
    public Iterable<Result<OriginalItem, Output>> process( final Iterable<Result<OriginalItem, Input>> inputs ) {
        final List<Result<OriginalItem, Input>> inputList = IterableUtils.asList( inputs );
        if ( inputList.isEmpty() ) {
            return Collections.emptyList();
        }
        try {
            return doPersist( inputList );
        } catch ( final VirtualMachineError e ) {
            // there is absolutely no way how those types of errors could be handled, rethrow it
            throw e;
        } catch ( final Throwable t ) {
            if ( inputList.size() == 1 ) {
                final Result<OriginalItem, Input> result = inputList.get( 0 );
                return Collections.singletonList( Result.<OriginalItem, Output> builder( result ).failed( t ) );
            }

            final List<Result<OriginalItem, Output>> retriedResults = new ArrayList<>( inputList.size() );
            for ( final Result<OriginalItem, Input> input : inputList ) {
                final Iterable<Result<OriginalItem, Output>> outputs = process( Collections.singletonList( input ) );
                if ( !outputs.iterator().hasNext() ) {
                    throw new IllegalStateException( "processing of singletons must never lead to empty lists here" );
                }
                outputs.forEach( retriedResults::add );
            }
            return Collections.unmodifiableList( retriedResults );
        }
    }

    private Iterable<Result<OriginalItem, Output>> doPersist( final Iterable<Result<OriginalItem, Input>> iterable ) {
        final List<Result<OriginalItem, Input>> successes = StreamSupport.stream( iterable.spliterator(), false )
                .filter( Result::isSuccess )
                .collect( Collectors.toList() );
        final List<Result<OriginalItem, Input>> fails = StreamSupport.stream( iterable.spliterator(), false )
                .filter( Result::isFailed )
                .collect( Collectors.toList() );

        final List<Input> outputs = getSuccessOutputs( successes );

        final List<Output> persistenceResults = outputs.isEmpty()
            ? Collections.emptyList()
            : apply( outputs );

        if ( persistenceResults.size() != outputs.size() || persistenceResults.size() != successes.size() ) {
            throw new IllegalStateException( "persistence results of unexpected size produced by " + this );
        }

        final List<Result<OriginalItem, Output>> b = new ArrayList<>( outputs.size() + fails.size() );
        for ( int i = 0; i < outputs.size(); i++ ) {
            final Result<OriginalItem, Input> processingResult = successes.get( i );
            final Output persistenceResult = persistenceResults.get( i );
            b.add( Result.<OriginalItem, Output> builder( processingResult ).withOutput( persistenceResult ).success() );
        }
        for ( final Result<OriginalItem, Input> failed : fails ) {
            b.add( Result.<OriginalItem, Output> builder( failed ).failed() );
        }
        return Collections.unmodifiableList( b );
    }

    protected abstract List<Output> apply( final List<Input> outputs );

    private List<Input> getSuccessOutputs( final List<Result<OriginalItem, Input>> results ) {
        final List<Input> b = new ArrayList<>( results.size() );
        for ( final Result<OriginalItem, Input> r : results ) {
            if ( r == null ) {
                throw new IllegalArgumentException( "Result was null in list " + results );
            }
            if ( r.isSuccess() ) {
                final Input o = r.getOutput();
                if ( o == null ) {
                    throw new IllegalArgumentException( "A successful Item must not have a null output" );
                }
                b.add( o );
            }
        }
        return Collections.unmodifiableList( b );
    }
}