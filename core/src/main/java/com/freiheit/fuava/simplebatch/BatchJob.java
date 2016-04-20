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
import com.google.common.base.Preconditions;
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
 * @param <OriginalInput>
 *            The data fetched by the fetcher
 * @param <Output>
 *            The result of the processor
 */
public class BatchJob<OriginalInput, Output> {
    private static final Logger LOG = LoggerFactory.getLogger( BatchJob.class );
    public static final int TERMINATION_TIMEOUT_HOURS = 96;
    public static final int PANIC_VM_ERROR = 1;

    private final class CallProcessor implements Consumer<List<Result<FetchedItem<OriginalInput>, OriginalInput>>> {
        private final DelegatingProcessingResultListener<OriginalInput, Output> listeners;
        private final PanicCallback panicCallback;

        private CallProcessor( final DelegatingProcessingResultListener<OriginalInput, Output> listeners, final PanicCallback panicCallback ) {
            this.listeners = listeners;
            this.panicCallback = Preconditions.checkNotNull( panicCallback );
        }

        @Override
        public void accept( final List<Result<FetchedItem<OriginalInput>, OriginalInput>> sourceResults ) {


            try {
                listeners.onFetchResults( sourceResults );
                final Iterable<? extends Result<FetchedItem<OriginalInput>, Output>> processingResults = persistence.process( sourceResults );
                listeners.onProcessingResults( processingResults );
            } catch ( final VirtualMachineError e ) {
                LOG.error( "FATAL: Exception went through the Processors. You need to ensure that this cannot happen, in order to achieve proper error handling " + e.getMessage(), e );
                /*
                 * There is no way we can get out of this. We need to ensure the entire program halts - thus we do a system
                 */
                panicCallback.panic( "Virtual Machine Error", PANIC_VM_ERROR );
            } catch ( final Throwable t ) {
                LOG.error( "FATAL: Exception went through the Processors. You need to ensure that this cannot happen, in order to achieve proper error handling" + t.getMessage(), t );
            }
        }
    }

    public static class Builder<OriginalInput, Output> {
        private int processingBatchSize = 1000;
        private boolean parallel = false;
        private Integer numParallelThreads = null;
        private int parallelTerminationTimeoutHours = TERMINATION_TIMEOUT_HOURS;
        private boolean printFinalTimeMeasures = true;
        private Fetcher<OriginalInput> fetcher;
        private PanicCallback panicCallback;
        private Processor<FetchedItem<OriginalInput>, OriginalInput, Output> processor;

        private final ArrayList<ProcessingResultListener<OriginalInput, Output>> listeners =
                new ArrayList<ProcessingResultListener<OriginalInput, Output>>();
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
        public Builder<OriginalInput, Output> setParallel( final boolean parallel ) {
            this.parallel = parallel;
            return this;
        }
        
        /**
         * Set the callback for 'panic' situations like virtual machine errors where it makes no sense to try and continue processing.
         * Default behaviour is, that System.exit() is called.
         * @param panicCallback The callback for panic situations
         * @return this instance for method chaining
         */
        public Builder<OriginalInput, Output> setPanicCallback( final PanicCallback panicCallback ) {
            this.panicCallback = panicCallback;
            return this;
        }
        

        /**
        * Set the number of hours this jobs waits for the spawned chunks to be processed. 
        * Applies only if you use {@link #setNumParallelThreads(Integer)}.
        * 
        * @return this for method chaining
        */
        public Builder<OriginalInput, Output> setParallelTerminationTimeoutHours( final int parallelTerminationTimeoutHours ) {
            this.parallelTerminationTimeoutHours = parallelTerminationTimeoutHours;
            return this;
        }
        
        public int getParallelTerminationTimeoutHours() {
            return parallelTerminationTimeoutHours;
        }
        
        /**
         * The number of threads to use for parallel processing. If set to null and parallel is set to true, Java 8 parallel streaming will be used.
         * @return this for method chaining
         */
        public Builder<OriginalInput, Output> setNumParallelThreads( final Integer numParallelThreads ) {
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
        public Builder<OriginalInput, Output>  setPrintFinalTimeMeasures( final boolean printFinalTimeMeasures ) {
            this.printFinalTimeMeasures = printFinalTimeMeasures;
            return this;
        }

        /**
        * How many items from the fetcher are put together in one chunk and processed together
        * @return this for method chaining
        */
        public Builder<OriginalInput, Output> setProcessingBatchSize( final int processingBatchSize ) {
            this.processingBatchSize = processingBatchSize;
            return this;
        }

        /**
        * The fetcher that produces the items to process. Should be fast
        * @return this for method chaining
        */
        public Builder<OriginalInput, Output> setFetcher( final Fetcher<OriginalInput> idsFetcher ) {
            this.fetcher = idsFetcher;
            return this;
        }

        public Fetcher<OriginalInput> getFetcher() {
            return fetcher;
        }

        /**
        * The processor for processing chunks of items which were produced by the fetcher. May be slow.
        * @return this for method chaining
        */
        public Builder<OriginalInput, Output> setProcessor( final Processor<FetchedItem<OriginalInput>, OriginalInput, Output> writer ) {
            this.processor = writer;
            return this;
        }

        public Processor<FetchedItem<OriginalInput>, OriginalInput, Output> getProcessor() {
            return processor;
        }

        /**
        * Add a listener to call when processing events happen
        * @return this for method chaining
        */
        public Builder<OriginalInput, Output> addListener( final ProcessingResultListener<OriginalInput, Output> listener ) {
            this.listeners.add( listener );
            return this;
        }

        /**
        * Add listeners to call when processing events happen
        * @return this for method chaining
        */
        public Builder<OriginalInput, Output> addListeners( final Collection<ProcessingResultListener<OriginalInput, Output>> listeners ) {
            this.listeners.addAll( listeners );
            return this;
        }

        public Builder<OriginalInput, Output> removeListener( final ProcessingResultListener<OriginalInput, Output> listener ) {
            this.listeners.remove( listener );
            return this;
        }

        public Builder<OriginalInput, Output> removeListeners( final Collection<ProcessingResultListener<OriginalInput, Output>> listeners ) {
            this.listeners.removeAll( listeners );
            return this;
        }

        public ArrayList<ProcessingResultListener<OriginalInput, Output>> getListeners() {
            return listeners;
        }

        /**
        * The Description of the job
        * @return this for method chaining
        */
        public Builder<OriginalInput, Output> setDescription( final String desc ) {
            this.description = desc;
            return this;
        }

        public BatchJob<OriginalInput, Output> build() {
            final PanicCallback panicCallback = getPanicCallback();
            return new BatchJob<OriginalInput, Output>( description, processingBatchSize, parallel, numParallelThreads, parallelTerminationTimeoutHours, fetcher, processor, printFinalTimeMeasures, listeners, panicCallback );
        }

        public PanicCallback getPanicCallback() {
            return this.panicCallback == null ? new DefaultPanicCallback() : this.panicCallback;
        }

        public String getDescription() {
            return description;
        }

    }

    private final int processingBatchSize;
    private final boolean parallel;
    private final Integer numParallelThreads;
    private final Fetcher<OriginalInput> fetcher;
    private final Processor<FetchedItem<OriginalInput>, OriginalInput, Output> persistence;

    private final List<ProcessingResultListener<OriginalInput, Output>> listeners;
    private final String description;
    private final boolean printFinalTimeMeasures;
    private final int parallelTerminationTimeoutHours;
    private final PanicCallback panicCallback;

    /**
     * Callback for severe error conditions which should lead to aborting the entire processing.
     * Called for {@link VirtualMachineError}s such as {@link OutOfMemoryError}.
     */
    public interface PanicCallback {
       void panic( String reason, int code ); 
    }
    
    /**
     * Default implementation of the panic callback which simply calls System.exit.
     */
    public static final class DefaultPanicCallback implements PanicCallback {

        @Override
        public void panic( final String reason, final int code ) {
            final String message = "System exit (" + code + ") due to: " + reason;
            LOG.error( message );
            System.err.println( message );
            System.exit( code );
        }
        
    }
    
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
            final int parallelTerminationTimeoutHours,
            final Fetcher<OriginalInput> fetcher,
            final Processor<FetchedItem<OriginalInput>, OriginalInput, Output> processor,
            final boolean printFinalTimeMeasures,
            final List<ProcessingResultListener<OriginalInput, Output>> listeners,
            final PanicCallback panicCallback
    ) {
        this.description = description;
        this.processingBatchSize = processingBatchSize;
        this.parallel = parallel;
        this.numParallelThreads = numParallelThreads;
        this.parallelTerminationTimeoutHours = parallelTerminationTimeoutHours;
        this.fetcher = fetcher;
        this.persistence = processor;
        this.printFinalTimeMeasures = printFinalTimeMeasures;
        this.listeners = ImmutableList.copyOf( listeners );
        this.panicCallback = Preconditions.checkNotNull( panicCallback, "Panic Callback must be set" );
    }

    public static <Input, Output> Builder<Input, Output> builder() {
        return new Builder<Input, Output>();
    }

    @CheckReturnValue
    public ResultStatistics run() {
        final ResultStatistics.Builder<OriginalInput, Output> resultBuilder = ResultStatistics.builder();

        final DelegatingProcessingResultListener<OriginalInput, Output> listeners =
                new DelegatingProcessingResultListener<OriginalInput, Output>(
                        ImmutableList.<ProcessingResultListener<OriginalInput, Output>> builder().add( resultBuilder ).addAll(
                                this.listeners ).build()
                );

        listeners.onBeforeRun( this.description );

        final Iterable<Result<FetchedItem<OriginalInput>, OriginalInput>> sourceIterable = fetcher.fetchAll();

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

    protected void process( final DelegatingProcessingResultListener<OriginalInput, Output> listeners,
            final Iterable<Result<FetchedItem<OriginalInput>, OriginalInput>> sourceIterable ) {
        if ( this.parallel && this.numParallelThreads != null && this.numParallelThreads.intValue() > 0 ) {
            processWithBlockingQueue( listeners, this.numParallelThreads, sourceIterable );
        } else {
            processWithStreams( listeners, this.parallel, sourceIterable );
        }
    }

    protected void processWithStreams( 
            final DelegatingProcessingResultListener<OriginalInput, Output> listeners,
            final boolean useParallelStream,
            final Iterable<Result<FetchedItem<OriginalInput>, OriginalInput>> sourceIterable ) {
        
        final Iterable<List<Result<FetchedItem<OriginalInput>, OriginalInput>>> partitions = Iterables.partition( sourceIterable, processingBatchSize );
        
        StreamSupport.stream( partitions.spliterator(), parallel ).forEach( new CallProcessor( listeners, panicCallback ) );
    }
    
    private void processWithBlockingQueue( 
            final DelegatingProcessingResultListener<OriginalInput, Output> listeners,
            final int numParallelThreads,
            final Iterable<Result<FetchedItem<OriginalInput>, OriginalInput>> sourceIterable ) {
        final ThreadGroup threadGroup = new ThreadGroup( "Simplebatch Processing" ) {
            @Override
            public void uncaughtException( final Thread t, final Throwable e ) {
                try {
                    LOG.error( e.getMessage(), e );
                } catch (final Throwable t2) {
                    BatchJob.this.panicCallback.panic( "Thread died while logging", PANIC_VM_ERROR );
                    return;
                }
                if ( e instanceof VirtualMachineError ) {
                    BatchJob.this.panicCallback.panic( "Thread died with VirtualMachineError", PANIC_VM_ERROR );
                }
            }
        };
        
        new BlockingQueueExecutor<OriginalInput>( 
                numParallelThreads, 
                processingBatchSize, 
                TimeUnit.HOURS.toMillis( this.parallelTerminationTimeoutHours ), 
                new CallProcessor( listeners, panicCallback ),
                threadGroup)
        .accept( sourceIterable );
    }
    
}