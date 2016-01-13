package com.freiheit.fuava.simplebatch.processor;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.collect.ImmutableList;

public class TimeLoggingProcessor<OriginalItem, Input, Output> implements Processor<OriginalItem, Input, Output> {
    public static final Logger JOB_PERFORMANCE_LOGGER = LoggerFactory.getLogger( "Job Performance Logger" );
    public static final String STAGE_ID_ALL = "Total";

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
        public static final Counts NOTHING = new Counts( 0, 0 );

        private final int items;
        private final long durationNanos;

        private Counts( final int items, final long durationNanos ) {
            this.items = items;
            this.durationNanos = durationNanos;
        }

        public int getItems() {
            return items;
        }

        public long getDurationNanos() {
            return durationNanos;
        }

        public Counts plus( final int items, final long durationNanos ) {
            return new Counts( this.items + items, this.durationNanos + durationNanos );
        }
    }

    private final List<Stage> stages;
    private final ConcurrentHashMap<String, Counts> counts;

    private TimeLoggingProcessor( final Processor<OriginalItem, Input, Output> processor ) {
        this.stages = fixStageIds( toStages( processor ) );
        this.counts = new ConcurrentHashMap<>();
    }

    public static <OriginalItem, Input, Output> Processor<OriginalItem, Input, Output> wrap(
            final Processor<OriginalItem, Input, Output> processor ) {
        if ( processor instanceof TimeLoggingProcessor ) {
            return processor;
        }
        return new TimeLoggingProcessor<OriginalItem, Input, Output>( processor );
    }

    private List<Stage> fixStageIds( final List<Stage> stages ) {
        final ImmutableList.Builder<Stage> b = ImmutableList.builder();

        for ( int i = 0; i < stages.size(); i++ ) {
            final Stage stage = stages.get( i );

            b.add( new Stage( String.format( "Stage %02d", i + 1 ), stage.getDisplayName(), stage.processor ) );
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
            return ImmutableList.of( new Stage( "", processor.getClass().getSimpleName(), processor ) );
        }
    }

    @SuppressWarnings( { "unchecked", "rawtypes" } )
    @Override
    public Iterable<Result<OriginalItem, Output>> process( final Iterable<Result<OriginalItem, Input>> input ) {
        final Iterable values = doProcess( input );

        // TODO: nicht jedes mal?
        logCounts( stages, counts );

        return values;
    }

    /**
     * Calls process for the delegated stages and collects processing duration
     * data.
     */
    @SuppressWarnings( { "unchecked", "rawtypes" } )
    private Iterable<Result<OriginalItem, Output>> doProcess( final Iterable<Result<OriginalItem, Input>> input ) {
        Iterable values = input;
        final long startAll = System.nanoTime();
        int numItemsMax = 0;

        for ( final Stage stage : stages ) {
            final CountingIterable countingIterable = new CountingIterable<>( values );
            final long start = System.nanoTime();
            values = stage.apply( countingIterable );
            final long stop = System.nanoTime();
            final int numItems = countingIterable.getMaxCount();
            numItemsMax = Math.max( numItemsMax, numItems );
            updateCounts( stage.getId(), numItems, stop - start );
        }

        final long stopAll = System.nanoTime();
        updateCounts( STAGE_ID_ALL, numItemsMax, stopAll - startAll );

        return values;
    }

    private void logCounts( final List<Stage> stages, final ConcurrentHashMap<String, Counts> counts ) {
        if ( !JOB_PERFORMANCE_LOGGER.isInfoEnabled() ) {
            return;
        }
        JOB_PERFORMANCE_LOGGER.info( "\n" + renderCounts( stages, counts ) );
    }

    static String renderCounts( final List<Stage> stages, final ConcurrentHashMap<String, Counts> counts ) {
        final StringBuilder sb = new StringBuilder();
        final Counts total = counts.getOrDefault( STAGE_ID_ALL, Counts.NOTHING );
        sb.append( renderCounts( STAGE_ID_ALL, total.items, total.durationNanos, "" ) ).append( "\n" );

        for ( int i = 0; i < stages.size(); i++ ) {
            final Stage stage = stages.get( i );
            final String stageId = stage.getId();
            final Counts stageCounts = counts.getOrDefault( stageId, Counts.NOTHING );
            sb.append(
                    renderCounts( stageId, stageCounts.items, stageCounts.durationNanos, stage.getDisplayName() ) ).append(
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

    static String renderCounts( final String name, final int items, final long durationNanos, final String desc ) {
        
        // Total Items, Total Duration, Durchschnitt Dauer pro Item
        final long secondsAll = TimeUnit.NANOSECONDS.toSeconds( durationNanos );
        final long minutesAll = TimeUnit.SECONDS.toMinutes( secondsAll );
        final long hoursAll = TimeUnit.MINUTES.toHours( minutesAll );
        final long seconds = secondsAll - TimeUnit.MINUTES.toSeconds( minutesAll );
        final long minutes = minutesAll - TimeUnit.HOURS.toMinutes( hoursAll );

        final long millisPerItemAll = TimeUnit.NANOSECONDS.toMillis( items == 0
            ? 0
            : durationNanos / items );
        final long secondsPerItemAll = TimeUnit.MILLISECONDS.toSeconds( millisPerItemAll );
        final long millisPerItem = millisPerItemAll - TimeUnit.SECONDS.toMillis( secondsPerItemAll );


        return String.format( "%s:\t %8d total | %2d h %2d min %2d sec | %2d sec %2d ms | %s ",
                 name, 
                items,
                 hoursAll, minutes, seconds,
                secondsPerItemAll, millisPerItem,
                desc );
         
    }

    private Counts updateCounts( final String stageId, final int numItems, final long durationNanos ) {
        return counts.compute( stageId, ( key, oldValue ) -> ( oldValue == null
            ? Counts.NOTHING
            : oldValue ).plus( numItems, durationNanos ) );
    }

}
