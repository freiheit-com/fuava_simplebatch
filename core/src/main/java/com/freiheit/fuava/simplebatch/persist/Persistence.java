package com.freiheit.fuava.simplebatch.persist;

import com.freiheit.fuava.simplebatch.result.Result;

public interface Persistence<Input, Output> {
	
	/**
	 * Write the processing results. Note that you can expect to only receive successful results.
	 */
	Iterable<? extends Result<Input, ?>> persist(Iterable<Result<Input, Output>> iterable);
}