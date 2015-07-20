package com.freiheit.fuava.simplebatch;

import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

public final class MapBasedBatchDownloader<I, O> implements Function<List<I>, List<O>> {
    private final Map<I, O> map;

    public MapBasedBatchDownloader( final Map<I, O> map ) {
        this.map = map;
    }

    @Override
    public List<O> apply( final List<I> arg0 ) {
        final ImmutableList.Builder<O> b = ImmutableList.builder();
        for ( final I i : arg0 ) {
            b.add( map.get( i ) );
        }
        return b.build();
    }

}