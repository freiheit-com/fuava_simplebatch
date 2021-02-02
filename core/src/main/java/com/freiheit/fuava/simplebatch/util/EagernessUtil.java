package com.freiheit.fuava.simplebatch.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class EagernessUtil {
    /**
     * It may seem useless at first do enforce eagerness, but the BatchJob
     * will try to collect and print statistics only if the iterable is a Collection.
     * 
     * If our input was a List we know it is not really lazy, so we can eagerly fetch it.
     */
    public static <R> Iterable<R> preserveEagerness( final Iterable<?> originalIterable, final Iterable<R> resultingIterable ) {
        if ( originalIterable instanceof Collection ) {
            final List<R> result = new ArrayList<>();
            resultingIterable.forEach( result::add );
            return Collections.unmodifiableList( result );
        }
        return resultingIterable;
    }

    /**
     * It may seem useless at first do enforce eagerness , but the BatchJob
     * will try to collect and print statistics only if the iterable is a Collection.
     */
    public static <R> Iterable<R> preserveEagerness( final Iterable<R> resultingIterable ) {
        final List<R> result = new ArrayList<>();
        resultingIterable.forEach( result::add );
        return Collections.unmodifiableList( result );
    }
}
