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
package com.freiheit.fuava.simplebatch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

import javax.annotation.CheckReturnValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fetch.Fetcher;
import com.freiheit.fuava.simplebatch.processor.Processor;
import com.freiheit.fuava.simplebatch.processor.TimeLoggingProcessor;
import com.freiheit.fuava.simplebatch.result.DelegatingProcessingResultListener;
import com.freiheit.fuava.simplebatch.result.ProcessingResultListener;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * Downloads - or more generally, processes - data in two stages, via iterables.
 *
 * The output of the first stage is used as input for the second stage.
 *
 * A typical usecase would be to fetch Ids or URLs of the data to download in
 * the first stage ('source'), and then to fetch the data in the second stage
 * ('process'), persisting the data and the progress information in the third
 * stage 'sink'.
 *
 * The downloader uses iterables and does the stage 2 downloading in batches, so
 * you can provide iterables that stream over some data source, efficiently
 * processing huge amounts of data.
 *
 * @author Klas Kalass <klas.kalass@freiheit.com>
 *
 * @param <Input>
 *            The data fetched in stage one
 * @param <Output>
 *            The data fetched instage two
 */
public class BatchJob<Input, Output> {
    static final Logger LOG = LoggerFactory.getLogger( BatchJob.class );
    private static final long TERMINATION_TIMEOUT_HOURS = 1;

    private final class CallProcessor implements Consumer<List<Result<FetchedItem<Input>, Input>>> {
        private final DelegatingProcessingResultListener<Input, Output> listeners;

        private CallProcessor( final DelegatingProcessingResultListener<Input, Output> listeners ) {
            this.listeners = listeners;
        }

        @Override
        public void accept( final List<Result<FetchedItem<Input>, Input>> sourceResults ) {

            listeners.onFetchResults( sourceResults );

            final Iterable<? extends Result<FetchedItem<Input>, Output>> processingResults = persistence.process( sourceResults );

            listeners.onProcessingResults( processingResults );
        }
    }

    public static class Builder<Input, Output> {
        private int processingBatchSize = 1000;
        private boolean parallel = false;
        private Integer numParallelThreads = null;
        private boolean printFinalTimeMeasures = true;
        private Fetcher<Input> fetcher;
        private Processor<FetchedItem<Input>, Input, Output> processor;

        private final ArrayList<ProcessingResultListener<Input, Output>> listeners =
                new ArrayList<ProcessingResultListener<Input, Output>>();
        private String description;

        public Builder() {
        }

        public int getProcessingBatchSize() {
            return processingBatchSize;
        }

        public boolean isParallel() {
            return parallel;
        }
        
        /**
        * Set to false to process in current thread. Set true to use multiple Threads for processing (each chunk is thread confined though).
        * @return this for method chaining
        */
        public Builder<Input, Output> setParallel( final boolean parallel ) {
            this.parallel = parallel;
            return this;
        }
        
        /**
         * The number of threads to use for parallel processing. If set to null and parallel is set to true, Java 8 parallel streaming will be used.
         * @return this for method chaining
         */
        public Builder<Input, Output> setNumParallelThreads( final Integer numParallelThreads ) {
            this.numParallelThreads = numParallelThreads;
            return this;
        }
        
        public Integer getNumParallelThreads() {
            return numParallelThreads;
        }
       
        
        public boolean isPrintFinalTimeMeasures() {
            return printFinalTimeMeasures;
        }
        
        /**
        * Whether or not the final performance measures should be printed after run has finished
        * @return this for method chaining
        */
        public Builder<Input, Output>  setPrintFinalTimeMeasures( final boolean printFinalTimeMeasures ) {
            this.printFinalTimeMeasures = printFinalTimeMeasures;
            return this;
        }

        /**
        * How many items from the fetcher are put together in one chunk and processed together
        * @return this for method chaining
        */
        public Builder<Input, Output> setProcessingBatchSize( final int processingBatchSize ) {
            this.processingBatchSize = processingBatchSize;
            return this;
        }

        /**
        * The fetcher that produces the items to process. Should be fast
        * @return this for method chaining
        */
        public Builder<Input, Output> setFetcher( final Fetcher<Input> idsFetcher ) {
            this.fetcher = idsFetcher;
            return this;
        }

        public Fetcher<Input> getFetcher() {
            return fetcher;
        }

        /**
        * The processor for processing chunks of items which were produced by the fetcher. May be slow.
        * @return this for method chaining
        */
        public Builder<Input, Output> setProcessor( final Processor<FetchedItem<Input>, Input, Output> writer ) {
            this.processor = writer;
            return this;
        }

        public Processor<FetchedItem<Input>, Input, Output> getProcessor() {
            return processor;
        }

        /**
        * Add a listener to call when processing events happen
        * @return this for method chaining
        */
        public Builder<Input, Output> addListener( final ProcessingResultListener<Input, Output> listener ) {
            this.listeners.add( listener );
            return this;
        }

        /**
        * Add listeners to call when processing events happen
        * @return this for method chaining
        */
        public Builder<Input, Output> addListeners( final Collection<ProcessingResultListener<Input, Output>> listeners ) {
            this.listeners.addAll( listeners );
            return this;
        }

        public Builder<Input, Output> removeListener( final ProcessingResultListener<Input, Output> listener ) {
            this.listeners.remove( listener );
            return this;
        }

        public Builder<Input, Output> removeListeners( final Collection<ProcessingResultListener<Input, Output>> listeners ) {
            this.listeners.removeAll( listeners );
            return this;
        }

        public ArrayList<ProcessingResultListener<Input, Output>> getListeners() {
            return listeners;
        }

        /**
        * The Description of the job
        * @return this for method chaining
        */
        public Builder<Input, Output> setDescription( final String desc ) {
            this.description = desc;
            return this;
        }

        public BatchJob<Input, Output> build() {
            return new BatchJob<Input, Output>( description, processingBatchSize, parallel, numParallelThreads, fetcher, processor, printFinalTimeMeasures, listeners );
        }

        public String getDescription() {
            return description;
        }

    }

    private final int processingBatchSize;
    private final boolean parallel;
    private final Integer numParallelThreads;
    private final Fetcher<Input> fetcher;
    private final Processor<FetchedItem<Input>, Input, Output> persistence;

    private final List<ProcessingResultListener<Input, Output>> listeners;
    private final String description;
    private final boolean printFinalTimeMeasures;

    /**
     * @param description The Description of the job
     * @param processingBatchSize How many items from the fetcher are put together in one chunk and processed together
     * @param parallel false: process in current thread. true: use multiple Threads for processing (each chunk is thread confined though)
     * @param numParallelThreads the number of threads to use for parallel processing. If null and parallel is set to true, Java 8 parallel streaming will be used.
     * @param fetcher The fetcher that produces the items to process. Should be fast
     * @param processor The processor for processing chunks of items which were produced by the fetcher. May be slow.
     * @param printFinalTimeMeasures Wether or not the final performance measures should be printed after run has finished
     * @param listeners Listeners to call when processing events happen
     */
    protected BatchJob(
            final String description,
            final int processingBatchSize,
            final boolean parallel,
            final Integer numParallelThreads,
            final Fetcher<Input> fetcher,
            final Processor<FetchedItem<Input>, Input, Output> processor,
            final boolean printFinalTimeMeasures,
            final List<ProcessingResultListener<Input, Output>> listeners ) {
        this.description = description;
        this.processingBatchSize = processingBatchSize;
        this.parallel = parallel;
        this.numParallelThreads = numParallelThreads;
        this.fetcher = fetcher;
        this.persistence = processor;
        this.printFinalTimeMeasures = printFinalTimeMeasures;
        this.listeners = ImmutableList.copyOf( listeners );
    }

    public static <Input, Output> Builder<Input, Output> builder() {
        return new Builder<Input, Output>();
    }

    @CheckReturnValue
    public ResultStatistics run() {
        final ResultStatistics.Builder<Input, Output> resultBuilder = ResultStatistics.builder();

        final DelegatingProcessingResultListener<Input, Output> listeners =
                new DelegatingProcessingResultListener<Input, Output>(
                        ImmutableList.<ProcessingResultListener<Input, Output>> builder().add( resultBuilder ).addAll(
                                this.listeners ).build()
                );

        listeners.onBeforeRun( this.description );

        final Iterable<Result<FetchedItem<Input>, Input>> sourceIterable = fetcher.fetchAll();

        if ( sourceIterable instanceof Collection && this.persistence instanceof TimeLoggingProcessor ) {
            // Iterables could be lazy, but if it is a collection it should not be lazy so we can
            // count and report the result of the prepare stage.
            final Collection<?> collection = ( Collection<?> ) sourceIterable;
            ( ( TimeLoggingProcessor<?,?,?> ) this.persistence ).addNumPreparedItems( 
                    collection.size(), 
                    FluentIterable.from( collection ).filter( o -> ( ( Result<?, ?> ) o ).isSuccess() ).size(),
                    FluentIterable.from( collection ).filter( o -> ( ( Result<?, ?> ) o ).isFailed() ).size()
            );
        }
        
        process( listeners, sourceIterable );

        listeners.onAfterRun();
        resultBuilder.setListenerDelegationFailures( listeners.hasDelegationFailures() );

        final ResultStatistics statistics = resultBuilder.build();
        
        if ( this.printFinalTimeMeasures && this.persistence instanceof TimeLoggingProcessor ) {
            ( ( TimeLoggingProcessor<?,?,?> ) this.persistence ).logFinalCounts();
        }
        
        return statistics;
    }

    protected void process( final DelegatingProcessingResultListener<Input, Output> listeners,
            final Iterable<Result<FetchedItem<Input>, Input>> sourceIterable ) {
        if ( this.parallel && this.numParallelThreads != null && this.numParallelThreads.intValue() > 0 ) {
            processWithExecutor( listeners, this.numParallelThreads, sourceIterable );
        } else {
            processWithStreams( listeners, this.parallel, sourceIterable );
        }
    }

    protected void processWithStreams( 
            final DelegatingProcessingResultListener<Input, Output> listeners,
            final boolean useParallelStream,
            final Iterable<Result<FetchedItem<Input>, Input>> sourceIterable ) {
        
        final Iterable<List<Result<FetchedItem<Input>, Input>>> partitions = Iterables.partition( sourceIterable, processingBatchSize );
        
        StreamSupport.stream( partitions.spliterator(), parallel ).forEach( new CallProcessor( listeners ) );
    }
    
    protected void processWithExecutor( 
            final DelegatingProcessingResultListener<Input, Output> listeners,
            final int numParallelThreads,
            final Iterable<Result<FetchedItem<Input>, Input>> sourceIterable ) {
        final ExecutorService executorService = Executors.newFixedThreadPool( numParallelThreads );
        try {
            final CallProcessor callProcessor = new CallProcessor( listeners );
            final Iterable<List<Result<FetchedItem<Input>, Input>>> partitions = Iterables.partition( sourceIterable, processingBatchSize );
            for (final List<Result<FetchedItem<Input>, Input>> chunk: partitions) {
                executorService.submit( () -> callProcessor.accept( chunk ) );
            }
        } finally {
            LOG.info( "Shutting down Executor" );
            executorService.shutdown();
            LOG.info( "Waiting for termination of submitted jobs (up to " + TERMINATION_TIMEOUT_HOURS + " hours)." );
            try {
                executorService.awaitTermination( TERMINATION_TIMEOUT_HOURS, TimeUnit.HOURS );
            } catch ( final InterruptedException e ) {
                throw new IllegalStateException( "Executor Service did not terminate normally", e );
            }
        }
    }
}