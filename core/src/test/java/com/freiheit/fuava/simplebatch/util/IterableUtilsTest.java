package com.freiheit.fuava.simplebatch.util;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertEquals;

/**
 * Tests the {@link IterableUtils}.
 */
public class IterableUtilsTest {
    @Test( dataProvider = "testIsEmptyDeterminesEmptyness" )
    public void testIsEmptyDeterminesEmptyness( final Iterable<String> input, boolean output ) {
        if ( output ) {
            assertTrue( IterableUtils.isEmpty( input ), "Expected the iterable to be empty!" );
        } else {
            assertFalse( IterableUtils.isEmpty( input ), "Expected the iterable to be nonempty!" );
        }
    }

    @DataProvider( name = "testIsEmptyDeterminesEmptyness" )
    public Object[][] provideEmptynessData() {
        return new Object[][] {
                { Collections.emptyList(), true },
                { Collections.emptySet(), true },
                { ( Iterable<String> ) Collections::emptyIterator, true },
                { Collections.singleton( "123" ), false },
                { Collections.singletonList( "123" ), false },
                { ( Iterable<String> ) () -> Collections.singletonList( "123" ).iterator(), false },
        };
    }

    @Test
    public void testAsListConvertsIterableToList() {
        assertEquals( IterableUtils.asList( Collections.emptyList() ), Collections.emptyList() );
        assertEquals( IterableUtils.asList( Collections.singletonList( "hello" ) ), Collections.singletonList( "hello" ) );
        assertEquals( IterableUtils.asList( Collections.emptySet() ), Collections.emptyList() );
        assertEquals( IterableUtils.asList( Collections.singleton( "hello" ) ), Collections.singletonList( "hello" ) );
        assertEquals( IterableUtils.asList( Collections::emptyIterator ), Collections.emptyList() );
        assertEquals( IterableUtils.asList( () -> Collections.singletonList( "hello" ).iterator() ), Collections.singletonList( "hello" ) );
    }

    @Test
    public void testPartitionReturnsEmptyIterableOnNullOrEmpty() {
        assertEquals( IterableUtils.partition( null, 5 ), Collections.emptyList() );
        assertEquals( IterableUtils.partition( Collections.emptySet(), 5 ), Collections.emptyList() );
        assertEquals( IterableUtils.partition( Collections::emptyIterator, 5 ), Collections.emptyList() );
    }

    @Test( expectedExceptions = { IllegalArgumentException.class } )
    public void testPartitionThrowsExceptionIfBatchSizeTooSmall() {
        IterableUtils.partition( Collections.singleton( "TestEntry" ), -5 );
    }

    @Test
    public void testPartitionCreatesBatches() {
        final List<Integer> items = Arrays.asList( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 );
        final Iterable<List<Integer>> batches = IterableUtils.partition( items, 3 );
        assertEquals( IterableUtils.asList( batches ), Arrays.asList(
                Arrays.asList( 1, 2, 3 ),
                Arrays.asList( 4, 5, 6 ),
                Arrays.asList( 7, 8, 9 ),
                Collections.singletonList( 10 )
        ) );
    }

    @Test
    public void testPartitionCreatesBatchesExactly() {
        final List<Integer> items = Arrays.asList( 1, 2, 3, 4, 5, 6, 7, 8, 9 );
        final Iterable<List<Integer>> batches = IterableUtils.partition( items, 3 );
        assertEquals( IterableUtils.asList( batches ), Arrays.asList(
                Arrays.asList( 1, 2, 3 ),
                Arrays.asList( 4, 5, 6 ),
                Arrays.asList( 7, 8, 9 )
        ) );
    }
}
