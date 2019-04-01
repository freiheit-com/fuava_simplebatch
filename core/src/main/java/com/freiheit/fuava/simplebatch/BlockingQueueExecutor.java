package com.freiheit.fuava.simplebatch;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

final class BlockingQueueExecutor<OriginalInput> implements Consumer<Iterable<Result<FetchedItem<OriginalInput>, OriginalInput>>> {
    private static final Logger LOG = LoggerFactory.getLogger( BlockingQueueExecutor.class );

    private final AtomicBoolean finished = new AtomicBoolean();
    private final int numParallelThreads;
    private final int processingBatchSize;
    private final LinkedBlockingQueue<List<Result<FetchedItem<OriginalInput>, OriginalInput>>> queue;
    private final Consumer<List<Result<FetchedItem<OriginalInput>, OriginalInput>>> processor;
    private final long terminationTimeoutMs;

    private final ThreadGroup threadGroup;

    private static final class BlockingQueueConsumer<OriginalInput> implements Runnable {
        private final AtomicBoolean finished;
        private final LinkedBlockingQueue<List<Result<FetchedItem<OriginalInput>, OriginalInput>>> queue;
        private final Consumer<List<Result<FetchedItem<OriginalInput>, OriginalInput>>> processor;

        public BlockingQueueConsumer(
                final AtomicBoolean aborted,
                final LinkedBlockingQueue<List<Result<FetchedItem<OriginalInput>, OriginalInput>>> queue,
                final Consumer<List<Result<FetchedItem<OriginalInput>, OriginalInput>>> processor
                ) {
            this.finished = aborted;
            this.queue = queue;
            this.processor = processor;
        }

        @Override
        public void run() {

            while ( true ) {
                // waiting up to one second for the queue to fill. Normally, we should never wait here.
                try {
                    final List<Result<FetchedItem<OriginalInput>,OriginalInput>> list = this.queue.poll( 1, TimeUnit.SECONDS );
                    if ( list != null ) {
                        processor.accept( list );
                    } else {
                        if ( finished.get() ) {
                            LOG.debug( "Did not find an item to process, and there will not be any more" );
                        } else {
                            LOG.debug( "Did not find an item to process, will try again" );
                        }
                    }
                } catch (final InterruptedException e) {
                    LOG.warn( "Terminating thread due to interruption without waiting for the queue to be empty" );
                    return;
                }
                /*
                 * There are no items available and there will not be any more items put into the queue - return
                 */
                if ( this.queue.isEmpty() && finished.get() ) {
                    LOG.info( "No more items, will finish" );
                    return;
                }
                // there should be more items, or at least there should be more items to become available when we are waiting longer
            }
        }
    }
    public BlockingQueueExecutor(
            final int numParallelThreads,
            final int processingBatchSize,
            final long terminationTimeoutMs,
            final Consumer<List<Result<FetchedItem<OriginalInput>, OriginalInput>>> processor,
            final ThreadGroup threadGroup
    ) {
        this.numParallelThreads = numParallelThreads;
        this.processingBatchSize = processingBatchSize;
        this.terminationTimeoutMs = terminationTimeoutMs;
        this.queue = new LinkedBlockingQueue<>( numParallelThreads * 2 );
        this.processor = processor;
        this.threadGroup = threadGroup;
    }


    @Override
    public void accept( final Iterable<Result<FetchedItem<OriginalInput>, OriginalInput>> sourceIterable ) {
        this.finished.set( false );

        // Start the consumers
        final List<Thread> threads = createThreads( numParallelThreads, threadGroup );

        threads.stream().forEach( t -> {LOG.debug( "Starting {}", t.getName() );t.start();} );

        final long maxEndTime = System.currentTimeMillis() + terminationTimeoutMs;

        final Iterable<List<Result<FetchedItem<OriginalInput>, OriginalInput>>> partitions = Iterables.partition( sourceIterable, processingBatchSize );
        int total = 0;
        for (final List<Result<FetchedItem<OriginalInput>, OriginalInput>> chunk: partitions) {
            total += chunk.size();
            LOG.info( "Adding chunk of " + chunk.size() + " items to the queue. Current total: " + total );
            try {
                final long timeoutMs = getTimeoutMs(maxEndTime);
                if (!queue.offer( chunk, timeoutMs, TimeUnit.MILLISECONDS ) ) {
                    LOG.error( "Aborting further processing because we could not add items to the queue within " + timeoutMs +  " ms" );
                }
            } catch ( final InterruptedException e ) {
                LOG.error( "Interruption while filling processing queue. Will not add any more items - note that we only process the first " + total + " items", e );
            }
        }

        // signal the threads that there will be no more items added to the queue
        this.finished.set( true );


        LOG.debug( "Shutting down Executor" );
        for ( final Thread t: threads ) {
            final long timeoutMs = getTimeoutMs(maxEndTime);
            LOG.debug( "Waiting for {} to terminate (up to {} ms)", t.getName(), timeoutMs );
            try {
                t.join( timeoutMs );
            } catch ( final Exception e ) {
                LOG.error( "Thread interrupted during termination - not all items might have been processed correctly", e );
            }
        }

        if (!queue.isEmpty()) {
            throw new IllegalStateException( "Processing did not finish within time and was aborted. Timeout was " + terminationTimeoutMs + " ms " );
        }

    }


    /**
     * The number of milliseconds we can at most wait - but ensures that always a certain minimum time is used for waiting even if the configured time is not available any more.
     * @param maxEndTime The end time we try to reach at most
     * @return the number ms to wait
     */
    private long getTimeoutMs( final long maxEndTime ) {
        return Math.max( maxEndTime - System.currentTimeMillis(), TimeUnit.SECONDS.toMillis( 1 ) );
    }


    private List<Thread> createThreads( final int numParallelThreads, final ThreadGroup group ) {
        final ImmutableList.Builder<Thread> threads = ImmutableList.builder();
        for ( int i = 0; i < numParallelThreads; i++ ) {
            final String threadName = "SBProc_" + Strings.padStart( Integer.toString( i ), 2, '0');
            LOG.debug( "Creating Processing Thread {}", threadName );
            final Thread t = new Thread(group, new BlockingQueueExecutor.BlockingQueueConsumer<>( finished, queue, processor ), threadName );
            t.setDaemon( false /* VM should not exit while this thread is still alive */ );
            threads.add( t );
        }
        return threads.build();
    }

}