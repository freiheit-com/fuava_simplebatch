package com.freiheit.fuava.simplebatch.processor;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

public class TimeLoggingProcessor<OriginalItem, Input, Output> implements Processor<OriginalItem, Input, Output> {
    public static final Logger JOB_PERFORMANCE_LOGGER = LoggerFactory.getLogger( "Job Performance Logger" );
    private static final String STAGE_ID_TOTAL = "Total   ";
    private static final String STAGE_ID_PREPARE = "Prepare ";
    

    private static final class Stage {
        private final String id;
        private final String name;
        private final Processor<?, ?, ?> processor;

        @SuppressWarnings( "rawtypes" )
        private Stage( final String id, final String name, final Processor processor ) {
            this.id = id;
            this.name = name;
            this.processor = processor;
        }

        @SuppressWarnings( { "rawtypes", "unchecked" } )
        public Iterable apply( final Iterable iterable ) {
            return processor.process( iterable );
        }

        public String getId() {
            return id;
        }

        public String getDisplayName() {
            return name;
        }

    }

    private static final class CountingIterable<T> implements Iterable<T> {
        private final Iterable<T> it;
        private final ConcurrentLinkedQueue<CountingIterator<T>> q = new ConcurrentLinkedQueue<>();

        public CountingIterable( final Iterable<T> it ) {
            this.it = it;
        }

        int getMaxCount() {
            int v = 0;
            for ( final CountingIterator<T> c : q ) {
                v = Math.max( v, c.getCounter() );
            }
            return v;
        }

        @Override
        public Iterator<T> iterator() {
            final CountingIterator<T> c = new CountingIterator<T>( it.iterator() );
            this.q.add( c );
            return c;
        }
    }

    private static final class CountingIterator<T> implements Iterator<T> {
        private final Iterator<T> it;
        private int counter = 0;

        public CountingIterator( final Iterator<T> it ) {
            this.it = it;
        }

        public int getCounter() {
            return counter;
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public T next() {
            final T value = it.next();
            counter++;
            return value;
        }
    }

    public static final class Counts {
        public static final int NUM_ITEMS_UNKNOWN = -1;
        public static final long NOT_INITIALIZED_NANOS = -1;
        public static final Counts NOTHING = new Counts( 0, Counts.NUM_ITEMS_UNKNOWN, Counts.NUM_ITEMS_UNKNOWN, 0 );

        private final int items;
        private final long durationNanos;
        private final int numSuccess;
        private final int numFailed;

        private Counts( final int items, final int numSuccess, final int numFailed, final long durationNanos ) {
            this.items = items;
            this.numSuccess = numSuccess;
            this.numFailed = numFailed;
            this.durationNanos = durationNanos;
        }

        public static Counts of( final int items, final int numSuccess, final int numFailed, final long durationNanos ) {
            return new Counts( items, numSuccess, numFailed, durationNanos );
        }

        public int getItems() {
            return items;
        }

        public long getDurationNanos() {
            return durationNanos;
        }

        public Counts plus( final int items, final int numSuccess, final int numFailed, final long durationNanos) {
            if ( this.items == NUM_ITEMS_UNKNOWN && items != NUM_ITEMS_UNKNOWN ) {
                throw new IllegalStateException( "Cannot add items to a counts instance with unknown items" );
            }
            return new Counts( 
                    addIfKnown( this.items, items ),
                    addIfKnown( this.numSuccess, numSuccess ),
                    addIfKnown( this.numFailed, numFailed ),
                    addDurationIfInitialized( this.durationNanos, durationNanos ) );
        }

        private int addIfKnown( final int oldNum, final int num ) {
            return num != NUM_ITEMS_UNKNOWN ? ( oldNum == NUM_ITEMS_UNKNOWN ? num : oldNum + num) : oldNum;
        }
        
        private long addDurationIfInitialized( final long oldNum, final long num ) {
            return num != NOT_INITIALIZED_NANOS ? ( oldNum == NOT_INITIALIZED_NANOS ? num : oldNum + num) : oldNum;
        }

        public int getNumSuccess() {
            return numSuccess;
        }

        public int getNumFailed() {
            return numFailed;
        }
    }

    private final AtomicLong lastloggedMillis = new AtomicLong( 0 );

    private final long minMillisBetweenLogging = TimeUnit.SECONDS.toMillis( 10 );

    private final List<Stage> stages;
    private final ConcurrentHashMap<String, Counts> counts;
    private final String stageIdTotal;
    private final String stageIdPrepare;
    private final long initializationNanos;
    private final AtomicLong processingStartNanos = new AtomicLong( Counts.NOT_INITIALIZED_NANOS );
    private final AtomicInteger totalCounter = new AtomicInteger( 0 );
    private final AtomicInteger totalSuccessCounter = new AtomicInteger( 0 );
    private final AtomicInteger totalFailCounter = new AtomicInteger( 0 );

    private TimeLoggingProcessor( final String prefix, final Processor<OriginalItem, Input, Output> processor ) {
        this.initializationNanos = System.nanoTime();
        this.stageIdTotal = buildStageName( prefix, STAGE_ID_TOTAL );
        this.stageIdPrepare = buildStageName( prefix, STAGE_ID_PREPARE );
        this.stages = fixStageIds( prefix, toStages( processor ) );
        this.counts = new ConcurrentHashMap<>();

    }

    private String buildStageName( final String prefix, final String name ) {
        return Strings.isNullOrEmpty( prefix )
            ? name
            : prefix + " " + name;
    }

    public static <OriginalItem, Input, Output> TimeLoggingProcessor<OriginalItem, Input, Output> wrap(
            final Processor<OriginalItem, Input, Output> processor ) {
        return wrap( "", processor );
    }

    public static <OriginalItem, Input, Output> TimeLoggingProcessor<OriginalItem, Input, Output> wrap(
            final String prefix,
            final Processor<OriginalItem, Input, Output> processor ) {
        if ( processor instanceof TimeLoggingProcessor ) {
            return (TimeLoggingProcessor<OriginalItem, Input, Output>) processor;
        }
        return new TimeLoggingProcessor<OriginalItem, Input, Output>( prefix, processor );
    }

    private List<Stage> fixStageIds( final String prefix, final List<Stage> stages ) {
        final ImmutableList.Builder<Stage> b = ImmutableList.builder();

        for ( int i = 0; i < stages.size(); i++ ) {
            final Stage stage = stages.get( i );

            b.add( new Stage( buildStageName( prefix, String.format( "Stage %02d", i + 1 ) ), stage.getDisplayName(),
                    stage.processor ) );
        }
        return b.build();
    }

    @SuppressWarnings( "rawtypes" )
    private List<Stage> toStages( final Processor processor ) {
        if ( processor instanceof ComposedProcessor ) {
            final ComposedProcessor cp = (ComposedProcessor) processor;
            return ImmutableList.<Stage> builder().addAll( toStages( cp.getFirst() ) ).addAll( toStages( cp.getSecond() ) ).build();
        } else if ( processor instanceof ChainedProcessor ) {
            final ChainedProcessor chained = (ChainedProcessor) processor;
            return toStages( chained.f );
        } else {
            return ImmutableList.of( new Stage( "", processor.getStageName(), processor ) );
        }
    }

    @SuppressWarnings( { "unchecked", "rawtypes" } )
    @Override
    public Iterable<Result<OriginalItem, Output>> process( final Iterable<Result<OriginalItem, Input>> input ) {

        final Iterable values = doProcess( input );

        // maximal alle x millisekunden loggen
        final long now = System.currentTimeMillis();
        final long last = lastloggedMillis.get();
        if ( now - last > minMillisBetweenLogging ) {
            lastloggedMillis.set( now );
            logCounts( stages, counts );
        }

        return values;
    }

    /**
     * Log the counts, but only if logging is enabled and total is greater than zero.
     * @return true if the final counts were logged
     */
    public boolean logFinalCounts() {
        if ( !JOB_PERFORMANCE_LOGGER.isInfoEnabled() ) {
            return false;
        }
        if ( counts.getOrDefault( stageIdTotal, Counts.NOTHING ).getItems() == 0 ) {
            return false;
        }
        JOB_PERFORMANCE_LOGGER.info( "\n******************************\n Final Job Performance Report \n******************************\n" + renderCounts( stages, counts ) );
        return true;
    }

    /**
     * Calls process for the delegated stages and collects processing duration
     * data.
     */
    @SuppressWarnings( { "unchecked", "rawtypes" } )
    private Iterable<Result<OriginalItem, Output>> doProcess( final Iterable<Result<OriginalItem, Input>> input ) {
        Iterable outputValues = input;
        final long processingBatchStartNanos = System.nanoTime();
        /* remember the time of very first call */
        final long processingStartNanos = this.processingStartNanos.get();
        if (processingStartNanos == Counts.NOT_INITIALIZED_NANOS) {
            if ( this.processingStartNanos.compareAndSet( Counts.NOT_INITIALIZED_NANOS, processingBatchStartNanos ) ) {
                addCounts( stageIdPrepare, Counts.NUM_ITEMS_UNKNOWN, Counts.NUM_ITEMS_UNKNOWN, Counts.NUM_ITEMS_UNKNOWN, processingBatchStartNanos - this.initializationNanos );
            }
        }
        int numItemsMax = 0;
        int numSuccessMin = Counts.NUM_ITEMS_UNKNOWN;
        int numFailedMax = 0;

        for ( final Stage stage : stages ) {
            final Iterable inputValues = prepareInputIterable( outputValues );
            final long start = System.nanoTime();
            
            outputValues = stage.apply( inputValues );
            
            final long stop = System.nanoTime();
            final int numTotal = getNumTotalItems( inputValues, outputValues );
            final int numSuccess = getNumFiltered( outputValues, Result::isSuccess );
            final int numFailed = getNumFiltered( outputValues, Result::isFailed );
            numItemsMax = Math.max( numItemsMax, numTotal );
            numSuccessMin = numSuccessMin == Counts.NUM_ITEMS_UNKNOWN ? numSuccess : Math.min( numSuccessMin, numSuccess );
            numFailedMax = Math.max( numFailedMax, numFailed );
            addCounts( 
                    stage.getId(), 
                    numTotal, 
                    numSuccess, 
                    numFailed, 
                    stop - start 
            );
        }

        final long processingBatchStopNanos = System.nanoTime();

        /**
         * The total stage does not simply add up the performance of the single
         * steps, but instead measures the total time which may be increased e.
         * g. by a fetcher or some other outside stuff, or decreased e. g. by
         * doing things in parallel.
         */
        final int totalItems = this.totalCounter.addAndGet( numItemsMax );
        final int totalSuccessItems = this.totalSuccessCounter.addAndGet( numSuccessMin );
        final int totalFailedItems = this.totalFailCounter.addAndGet( numFailedMax );
        replaceCounts( stageIdTotal, totalItems, totalSuccessItems, totalFailedItems, processingBatchStopNanos - this.processingStartNanos.get() );

        return outputValues;
    }

    @SuppressWarnings( "rawtypes" ) 
    private int getNumTotalItems( final Iterable inputValues, final Iterable outputValues ) {
        if ( outputValues instanceof Collection ) {
            // the best option: count how many Items were produced by the stage
            return ( ( Collection<?> ) outputValues ).size();
        }
        // Fallback: If the outputValues cannot be safely counted without side effects, return the input counts
        if ( inputValues instanceof Collection ) {
            return ( ( Collection<?> ) inputValues ).size();
        }
        // during prepareInputIterable there has been a CountingIterable created if it is not a collection 
        return ( ( CountingIterable<?> ) inputValues ).getMaxCount(); 
    }

    @SuppressWarnings( "rawtypes" ) 
    private int getNumFiltered( final Iterable outputValues, final Predicate<Result<?, ?>> filter ) {
        if ( outputValues instanceof Collection ) {
            // the best option: count how many success Items were produced by the stage
            // We assume here that collections will not have bad side effects, but are prepared for 
            // pure Iterables to be non-safe for multiple iterations
            return FluentIterable
                    .from( ( Collection<?> ) outputValues )
                    .filter( o -> {
                        final Result<?, ?> ri = (Result<?, ?>)o;
                        return filter.test( ri );
                    } )
                    .size();
        }
        return Counts.NUM_ITEMS_UNKNOWN;
    }

    @SuppressWarnings( { "rawtypes", "unchecked" } )
    private Iterable prepareInputIterable( final Iterable values ) {
        // if it is a collection, we do not need to do anything and simply return the collection
        if ( values instanceof Collection ) {
            return values;
        }
        // if it is not, we wrap it with some code that at least counts how many Items are processed
        // while iterating.
        return new CountingIterable<>( values );
    }

    private void logCounts( final List<Stage> stages, final ConcurrentHashMap<String, Counts> counts ) {
        if ( !JOB_PERFORMANCE_LOGGER.isInfoEnabled() ) {
            return;
        }
        JOB_PERFORMANCE_LOGGER.info( "\n" + renderCounts( stages, counts ) );
    }

    private String renderCounts( final String id, final Counts counts, final String displayName ) {
        return renderCounts( 
                id,  
                counts.getItems(), 
                counts.getNumSuccess(), 
                counts.getNumFailed(), 
                counts.getDurationNanos(),
                displayName
        );
    }
    String renderCounts( final List<Stage> stages, final ConcurrentHashMap<String, Counts> counts ) {
        final StringBuilder sb = new StringBuilder();
        final Counts prepare = counts.getOrDefault( stageIdPrepare, Counts.NOTHING );
        final Counts total = counts.getOrDefault( stageIdTotal, Counts.NOTHING );
        sb.append( renderCounts( stageIdPrepare, prepare, "" ) ).append( "\n" );
        sb.append( renderCounts( stageIdTotal, total, "" ) ).append( "\n" );

        for ( int i = 0; i < stages.size(); i++ ) {
            final Stage stage = stages.get( i );
            final String stageId = stage.getId();
            final Counts stageCounts = counts.getOrDefault( stageId, Counts.NOTHING );
            sb.append(
                    renderCounts( stageId, stageCounts, stage.getDisplayName() ) ).append(
                            "\n" );
        }
        return sb.toString();
    }

    /**
     * Expose the current counts for testing or reporting.
     * 
     * Note that the underlying datastructure is a concurrent hash map and
     * concurrent updates may happen.
     */
    public Map<String, Counts> getCurrentCounts() {
        return java.util.Collections.unmodifiableMap( counts );
    }

    static String renderCounts( 
            final String name, 
            final int items, 
            final int numSuccess, 
            final int numFailed, 
            final long durationNanos, 
            final String desc 
    ) {
        
        // Total Items, Total Duration, Durchschnitt Dauer pro Item
        final long secondsAll = TimeUnit.NANOSECONDS.toSeconds( durationNanos );
        final long minutesAll = TimeUnit.SECONDS.toMinutes( secondsAll );
        final long hoursAll = TimeUnit.MINUTES.toHours( minutesAll );
        final long seconds = secondsAll - TimeUnit.MINUTES.toSeconds( minutesAll );
        final long minutes = minutesAll - TimeUnit.HOURS.toMinutes( hoursAll );

        final long millisPerItemAll = TimeUnit.NANOSECONDS.toMillis( items == 0 || items == Counts.NUM_ITEMS_UNKNOWN
            ? 0
            : durationNanos / items );
        final long secondsPerItemAll = TimeUnit.MILLISECONDS.toSeconds( millisPerItemAll );
        final long millisPerItem = millisPerItemAll - TimeUnit.SECONDS.toMillis( secondsPerItemAll );

        final double durationHours = (double) durationNanos / (double) TimeUnit.HOURS.toNanos( 1 );
        final double itemsPerHour = ( durationNanos == 0 || items == Counts.NUM_ITEMS_UNKNOWN )
            ? 0.0
            : items / durationHours;
        final String countsString = renderCountsString( items, numSuccess, numFailed ); 
        
        return String.format( "%s:\t %s | %2d h %2d min %2d sec | %2d sec %3d ms | %10.0f items / hour | %s ",
                 name, 
                 countsString,
                 hoursAll, minutes, seconds,
                secondsPerItemAll, millisPerItem,
                itemsPerHour,
                desc );
         
    }

    private static String renderCountsString( final int items, final int numSuccess, final int numFailed ) {
        if ( items == Counts.NUM_ITEMS_UNKNOWN ) {
            return "                    ";
        }
        if ( ( numSuccess == Counts.NUM_ITEMS_UNKNOWN && numFailed == Counts.NUM_ITEMS_UNKNOWN )
                || numFailed == 0 && numSuccess == items
        ) {
            return String.format( "%8d total |         ", items );
        }
        return String.format( "%8d total | %4d err", items , numFailed);
    }

    private Counts replaceCounts( final String stageId, final int numItems, final int numSuccess, final int numFailed, final long nanos ) {
        return counts.put( stageId, new Counts( numItems, numSuccess, numFailed, nanos ) );
    }

    private Counts addCounts( final String stageId, final int numItems, final int numSuccess, final int numFailed, final long durationNanos ) {
        return counts.compute( stageId, ( key, oldValue ) -> ( oldValue == null
            ? Counts.NOTHING
            : oldValue ).plus( numItems, numSuccess, numFailed, durationNanos ) );
    }

    public String getStageIdTotal() {
        return stageIdTotal;
    }

    public String getStageIdPrepare() {
        return stageIdPrepare;
    }


    /**
     * Must be called before processing start
     */
    public void addNumPreparedItems( final int numPreparedItems, final int numSuccess, final int numFailed ) {
        addCounts( stageIdPrepare, numPreparedItems, numSuccess, numFailed, Counts.NOT_INITIALIZED_NANOS );
    }

}
