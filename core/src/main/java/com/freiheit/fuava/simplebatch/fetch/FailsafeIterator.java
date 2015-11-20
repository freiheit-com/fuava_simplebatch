package com.freiheit.fuava.simplebatch.fetch;

import java.util.Iterator;

import com.freiheit.fuava.simplebatch.result.Result;

public final class FailsafeIterator<T> implements Iterator<Result<FetchedItem<T>, T>> {

    private final Iterator<T> iterator;
    private Result<FetchedItem<T>, T> forceNextElement;
    private Boolean forceHasNext;
    private int num = FetchedItem.FIRST_ROW;

    public FailsafeIterator( final Iterator<T> iterator ) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        if ( forceHasNext != null ) {
            return forceHasNext.booleanValue();
        }
        try {
            return iterator.hasNext();
        } catch ( final Throwable t ) {
            forceHasNext = Boolean.TRUE;
            forceNextElement = Result.failed( nextFetchedItem( null ), "Failed to call hasNext on delegate iterator", t );
            return forceHasNext.booleanValue();
        }
    }

    private FetchedItem<T> nextFetchedItem( final T value ) {
        return FetchedItem.of( value, num++ );
    }

    @Override
    public Result<FetchedItem<T>, T> next() {
        if ( forceNextElement != null ) {
            final Result<FetchedItem<T>, T> r = forceNextElement;
            forceHasNext = Boolean.FALSE;
            return r;
        }
        try {
            final T value = iterator.next();
            return Result.success( nextFetchedItem( value ), value );
        } catch ( final Throwable t ) {
            return Result.failed( nextFetchedItem( null ), "Failed to call next for delegate iterator", t );
        }
    }

}