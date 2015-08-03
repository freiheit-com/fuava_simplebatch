package com.freiheit.fuava.simplebatch.processor;

import com.freiheit.fuava.simplebatch.result.Result;

public interface Processor<Input, Data, Persisted> {

    /**
     * Write the processing results. Note that you can not expect to only
     * receive successful results, be prepared for failed results.
     * 
     * <p>
     * <b>Important I:</b> Note that your implementation <b>MUST</b> return
     * exactly one instance of Result for each Result item of the input
     * iterable.
     * </p>
     * 
     * <p>
     * <b>Important II:</b> Further note, that your implementation <b>MUST
     * NOT</b> throw any Throwables (for example, no
     * {@link NullPointerException}s or other {@link RuntimeException}!), but
     * <b>MUST<b> return an iterable with a failed result instead.
     * </p>
     * 
     */
    Iterable<Result<Input, Persisted>> process( Iterable<Result<Input, Data>> iterable );
}