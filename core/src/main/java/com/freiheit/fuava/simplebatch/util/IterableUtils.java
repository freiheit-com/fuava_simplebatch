package com.freiheit.fuava.simplebatch.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Helper functions to better deal with Iterables.
 * @author Matthias Bender (matthias.bender@freiheit.com)
 */
public class IterableUtils {
    /**
     * Checks whether a given Iterable is actually empty.
     * @param iterable The Iterable to check.
     * @return true will be returned if there is no item to be iterated over. Otherwise false.
     */
    public static <T> boolean isEmpty( final Iterable<T> iterable ) {
        if ( iterable instanceof Collection ) {
            return ( (Collection<T>) iterable ).isEmpty();
        }
        return !iterable.iterator().hasNext();
    }

    /**
     * Converts a given iterable to a List. If the Iterable already is of type List it will simply
     * be returned.
     * @return A new (immutable) list is being returned unless the given iterable already is a list
     * in which case it will return the same instance.
     */
    public static <T> List<T> asList( final Iterable<T> iterable ) {
        if ( iterable instanceof List ) {
            return (List<T>) iterable;
        }
        final List<T> results = new ArrayList<>();
        iterable.forEach( results::add );
        return Collections.unmodifiableList( results );
    }

    /**
     * Constructs a new Iterable of batches of a given size. These batches are not created right away but
     * collected from the base iterable when iterating over it. If the number of items cannot be devided
     * by the given batchSize the last batch may be smaller than the batchSize requested. Every returned
     * batch is immutable.
     * @param iterable The iterable to collect the data from. If null or empty an empty list will be returned.
     * @param batchSize This batchSize has to be larger tha 0! It determines how many items can be found in each batch.
     */
    public static <T> Iterable<List<T>> partition( final Iterable<T> iterable, final int batchSize ) {
        if ( iterable == null ) {
            return Collections.emptyList();
        }
        if ( batchSize <= 0 ) {
            throw new IllegalArgumentException( "Lists cannot be partioned with negative batch sizes! [batchsize=" + batchSize + "]" );
        }
        return () -> new BatchedIterator<>( iterable, batchSize );
    }

    /**
     * Iterator to iterate over all items of a given Iterable in batches of fixed size.
     * @author Matthias Bender (matthias.bender@freiheit.com)
     */
    static class BatchedIterator<T> implements Iterator<List<T>> {
        private final Iterator<T> data;
        private final int batchSize;

        /**
         * Ctor.
         */
        public BatchedIterator( final Iterable<T> data, final int batchSize ) {
            this.data = data.iterator();
            this.batchSize = batchSize;
        }

        @Override
        public boolean hasNext() {
            return data.hasNext();
        }

        @Override
        public List<T> next() {
            if ( !hasNext() ) {
                throw new NoSuchElementException();
            }
            final List<T> batch = new ArrayList<>( batchSize );
            for ( int i = 0; i < batchSize && hasNext(); i++ ) {
                batch.add( data.next() );
            }
            return Collections.unmodifiableList( batch );
        }
    }
}
