package com.freiheit.fuava.simplebatch.fetch;

import com.freiheit.fuava.simplebatch.result.Result;


public interface Fetcher<T> {

    /**
     * Fetch the input for the processing stage.
     *
     * <b>Note</b> that implementations function <b>MUST NOT</b>
     * throw any Exceptions (no RuntimeExceptions, no Throwables etc)!
     *
     * @return an Iterable over the input, possibliy containing failed input results.
     */
    public Iterable<Result<FetchedItem<T>, T>> fetchAll();
}