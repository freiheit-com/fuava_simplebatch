package com.freiheit.fuava.simplebatch.processor;

import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TimeLoggingProcessorTest {

    @DataProvider
    public Object[][] durationAsStringsTestData() {
        return new Object[][] {
                { "All", "", 0, 0, "All:\t        0 total |  0 h  0 min  0 sec |  0 sec  0 ms |  " },
                { "All", "", 1, TimeUnit.SECONDS.toNanos( 1 ), "All:\t        1 total |  0 h  0 min  1 sec |  1 sec  0 ms |  " },
                { "All", "", 1, TimeUnit.MINUTES.toNanos( 1 ), "All:\t        1 total |  0 h  1 min  0 sec | 60 sec  0 ms |  " },
                { "All", "", 60, TimeUnit.HOURS.toNanos( 1 ), "All:\t       60 total |  1 h  0 min  0 sec | 60 sec  0 ms |  " },
                { "All", "", 1, TimeUnit.SECONDS.toNanos( 25 ) + TimeUnit.MINUTES.toNanos( 23 ) + TimeUnit.HOURS.toNanos( 2 ),
                        "All:\t        1 total |  2 h 23 min 25 sec | 8605 sec  0 ms |  " }
        };
    }

    @Test( dataProvider = "durationAsStringsTestData" )
    public void testDurationAsStrings( final String id, final String name, final int items, final long nanos,
            final String expected ) {
        final String result = TimeLoggingProcessor.renderCounts( id, items, nanos, name );
        Assert.assertEquals( result, expected );
  }
}
