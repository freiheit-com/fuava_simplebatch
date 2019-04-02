/*******************************************************************************
 * Copyright (c) 2019 freiheit.com technologies gmbh
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
 *
 * @author: sami.emad@freiheit.com
 ******************************************************************************/

package com.freiheit.fuava.simplebatch.processor;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

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
public abstract class RetryingResultProcessor<OriginalItem, Input, Output>
        implements Processor<OriginalItem, Input, Output> {
    private static final Logger LOG = LoggerFactory.getLogger( RetryingResultProcessor.class );

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
    public RetryingResultProcessor() {
    }

    @Override
    public Iterable<Result<OriginalItem, Output>> process( final Iterable<Result<OriginalItem, Input>> inputs ) {

        final List<Result<OriginalItem, Input>> inputList = ImmutableList.copyOf( inputs );
        if ( inputList.isEmpty() ) {
            return ImmutableList.of();
        }
        try {
            return doPersist( inputList );
        } catch ( final VirtualMachineError e ) {
            // there is absolutely no way how those types of errors could be handled, rethrow it
            throw e;
        } catch ( final Throwable t ) {
            if ( inputList.size() == 1 ) {
                final Result<OriginalItem, Input> result = inputList.get( 0 );
                return ImmutableList.of( Result.<OriginalItem, Output> builder( result ).failed( t ) );
            }
            LOG.info( "Caught Exception during processing of batch with " + inputList.size()
                    + " items, will RETRY in single item batches", t );
            final ImmutableList.Builder<Result<OriginalItem, Output>> retriedResults = ImmutableList.builder();
            for ( final Result<OriginalItem, Input> input : inputList ) {
                final List<Result<OriginalItem, Output>> outputs = ImmutableList.copyOf( process( ImmutableList.of( input ) ) );
                if ( outputs.size() != 1 ) {
                    throw new IllegalStateException( "processing of singletons must return exactly one item, "
                        + "but " + outputs.size() + " were returned." );
                }
                retriedResults.addAll( outputs );
            }
            return retriedResults.build();
        }
    }

    private Iterable<Result<OriginalItem, Output>> doPersist( final Iterable<Result<OriginalItem, Input>> iterable ) {
        final ImmutableList<Result<OriginalItem, Input>> successes = FluentIterable.from( iterable ).filter( Result::isSuccess ).toList();
        final ImmutableList<Result<OriginalItem, Input>> fails = FluentIterable.from( iterable ).filter( Result::isFailed ).toList();

        final List<Output> persistenceResults = successes.isEmpty()
            ? ImmutableList.of()
            : apply( successes );

        if ( persistenceResults.size() != successes.size() ) {
            throw new IllegalStateException( "persistence results of unexpected size produced by " + this );
        }
        final ImmutableList.Builder<Result<OriginalItem, Output>> b = ImmutableList.builder();

        for ( int i = 0; i < successes.size(); i++ ) {
            final Result<OriginalItem, Input> processingResult = successes.get( i );
            final Output persistenceResult = persistenceResults.get( i );
            b.add( Result.<OriginalItem, Output> builder( processingResult ).withOutput( persistenceResult ).success() );
        }

        for ( final Result<OriginalItem, Input> failed : fails ) {
            b.add( Result.<OriginalItem, Output> builder( failed ).failed() );
        }

        return b.build();
    }

    protected abstract List<Output> apply( final List<Result<OriginalItem, Input>> input );
}