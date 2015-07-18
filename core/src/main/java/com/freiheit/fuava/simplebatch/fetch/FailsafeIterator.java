package com.freiheit.fuava.simplebatch.fetch;

import java.util.Iterator;

import com.freiheit.fuava.simplebatch.result.Result;

public final class FailsafeIterator<T> implements Iterator<Result<T, T>> {

    private final Iterator<T> iterator;
    private Result<T, T> forceNextElement;
    private Boolean forceHasNext;

    public FailsafeIterator(Iterator<T> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        if (forceHasNext != null) {
            return forceHasNext.booleanValue();
        }
        try {
            return iterator.hasNext();
        } catch (Throwable t) {
            forceHasNext = Boolean.TRUE;
            forceNextElement = Result.failed(null, "Failed to call hasNext on delegate iterator", t);
            return forceHasNext.booleanValue();
        }
    }

    @Override
    public Result<T, T> next() {
        if (forceNextElement != null) {
            Result<T, T> r = forceNextElement;
            forceHasNext = Boolean.FALSE;
            return r;
        }
        try {
            T value = iterator.next();
            return Result.success(value, value);
        } catch (Throwable t) {
            return Result.failed(null, "Failed to call next for delegate iterator" , t);
        }
    }


}