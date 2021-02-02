package com.freiheit.fuava.simplebatch.util;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class CollectionUtils {
    @SafeVarargs
    public static <T> Set<T> asSet( final T item1, final T ... items ) {
        final Set<T> result = new LinkedHashSet<>( items.length + 1 );
        result.add( item1 );
        Collections.addAll( result, items );
        return result;
    }

    public static <K, V> Map<K, V> asMap(
            final K k1, final V v1,
            final K k2, final V v2,
            final K k3, final V v3 ) {
        final Map<K, V> result = new LinkedHashMap<>( 4 );
        result.put( k1, v1 );
        result.put( k2, v2 );
        result.put( k3, v3 );
        return result;
    }

    public static <K, V> Map<K, V> asMap(
            final K k1, final V v1,
            final K k2, final V v2,
            final K k3, final V v3,
            final K k4, final V v4 ) {
        final Map<K, V> result = new LinkedHashMap<>( 4 );
        result.put( k1, v1 );
        result.put( k2, v2 );
        result.put( k3, v3 );
        result.put( k4, v4 );
        return result;
    }
}
