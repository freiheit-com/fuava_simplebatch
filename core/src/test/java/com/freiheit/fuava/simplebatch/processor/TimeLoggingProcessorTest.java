package com.freiheit.fuava.simplebatch.processor;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.freiheit.fuava.simplebatch.processor.TimeLoggingProcessor.Counts;
import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class TimeLoggingProcessorTest {

    @DataProvider
    public Object[][] durationAsStringsTestData() {
        return new Object[][] {
                { "All", "", 0, 0, "All:\t        0 total |  0 h  0 min  0 sec |  0 sec  0 ms |          0 items / hour |  " },
                { "All", "", 1, TimeUnit.SECONDS.toNanos( 1 ),
                        "All:\t        1 total |  0 h  0 min  1 sec |  1 sec  0 ms |       3600 items / hour |  " },
                { "All", "", 1, TimeUnit.MINUTES.toNanos( 1 ),
                        "All:\t        1 total |  0 h  1 min  0 sec | 60 sec  0 ms |         60 items / hour |  " },
                { "All", "", 60, TimeUnit.HOURS.toNanos( 1 ),
                        "All:\t       60 total |  1 h  0 min  0 sec | 60 sec  0 ms |         60 items / hour |  " },
                { "All", "", 1, TimeUnit.SECONDS.toNanos( 25 ) + TimeUnit.MINUTES.toNanos( 23 ) + TimeUnit.HOURS.toNanos( 2 ),
                        "All:\t        1 total |  2 h 23 min 25 sec | 8605 sec  0 ms |          0 items / hour |  " }
        };
    }

    @Test( dataProvider = "durationAsStringsTestData" )
    public void testDurationAsStrings( final String id, final String name, final int items, final long nanos,
            final String expected ) {
        final String result = TimeLoggingProcessor.renderCounts( id, items, nanos, name );
        Assert.assertEquals( result, expected );
    }

    private static final class AddA extends SingleItemProcessor<String, String, String> {

        @Override
        protected String apply( final String input ) {
            return input + "a";
        }

    }

    private static final class AddB extends SingleItemProcessor<String, String, String> {

        @Override
        protected String apply( final String input ) {
            return input + "b";
        }

    }

    private static final class AddC extends SingleItemProcessor<String, String, String> {

        @Override
        protected String apply( final String input ) {
            return input + "c";
        }

    }

    private static final class AddD extends SingleItemProcessor<String, String, String> {

        @Override
        protected String apply( final String input ) {
            return input + "d";
        }

    }

    @Test
    public void testProcessorChainSingle() {
        final TimeLoggingProcessor<String, String, String> processor = wrap( new AddA() );
        assertResults( processor.process( data( "2", "3" ) ), "2a", "3a" );
        final Map<String, Counts> counts = processor.getCurrentCounts();
        Assert.assertEquals( ImmutableSet.copyOf( counts.keySet() ),
                ImmutableSet.of( processor.getStageIdTotal(), processor.getStageIdPrepare(), "Stage 01" ) );

        assertCountsItems( processor, counts, 2 );

    }

    @Test
    public void testProcessorChainWrapTwice() {
        final AddA a = new AddA();
        final Processor<String, String, String> processor = TimeLoggingProcessor.wrap( a );
        Assert.assertTrue( a != processor );
        final Processor<String, String, String> processor2 = TimeLoggingProcessor.wrap( processor );
        Assert.assertTrue( processor == processor2 );

    }

    @Test
    public void testProcessorChainMultiple() {
        final TimeLoggingProcessor<String, String, String> processor =
                wrap( new AddA().then( new AddB() ).then( new AddC() ).then( new AddD() ) );
        assertResults( processor.process( data( "2", "3" ) ), "2abcd", "3abcd" );
        final Map<String, Counts> counts = processor.getCurrentCounts();
        Assert.assertEquals( ImmutableSet.copyOf( counts.keySet() ),
                ImmutableSet.of( processor.getStageIdTotal(), processor.getStageIdPrepare(), "Stage 01", "Stage 02", "Stage 03",
                        "Stage 04" ) );
        assertCountsItems( processor, counts, 2 );
    }

    private void assertCountsItems( final TimeLoggingProcessor<String, String, String> processor,
            final Map<String, Counts> counts, final int expectedCount ) {
        for ( final Map.Entry<String, Counts> e : counts.entrySet() ) {
            final String key = e.getKey();
            final Counts c = e.getValue();
            if ( !key.equals( processor.getStageIdPrepare() ) ) {
                Assert.assertEquals( c.getItems(), expectedCount );
            }
        }
    }

    @Test
    public void testProcessorChainMultipleCompose() {
        final TimeLoggingProcessor<String, String, String> processor =
                wrap( Processors.compose( new AddD(),
                        Processors.compose( Processors.compose( new AddC(), new AddB() ), new AddA() ) ) );

        assertResults( processor.process( data( "2", "3" ) ), "2abcd", "3abcd" );
        final Map<String, Counts> counts = processor.getCurrentCounts();
        Assert.assertEquals( ImmutableSet.copyOf( counts.keySet() ),
                ImmutableSet.of( processor.getStageIdTotal(), processor.getStageIdPrepare(), "Stage 01", "Stage 02", "Stage 03",
                        "Stage 04" ) );
        assertCountsItems( processor, counts, 2 );
    }

    private void assertResults( final Iterable<Result<String, String>> results, final String... expected ) {
        int i = 0;
        for ( final Result<String, String> r : results ) {
            final String exp = expected[i];
            final String actual = r.getOutput();
            Assert.assertTrue( r.isSuccess() );
            Assert.assertEquals( actual, exp );
            i++;
        }
    }

    private Iterable<Result<String, String>> data( final String... values ) {
        final ImmutableList.Builder<Result<String, String>> b = ImmutableList.builder();
        for ( final String v : values ) {
            b.add( Result.success( v, v ) );
        }
        return b.build();
    }

    private TimeLoggingProcessor<String, String, String> wrap( final Processor<String, String, String> addA ) {
        return TimeLoggingProcessor.wrap( addA );
    }

}
