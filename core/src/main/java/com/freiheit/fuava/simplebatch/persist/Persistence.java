package com.freiheit.fuava.simplebatch.persist;

import com.freiheit.fuava.simplebatch.result.Result;

public interface Persistence<Input, Data, Persisted> {
	
	/**
	 * Write the processing results. Note that you can expect to only receive successful results.
	 */
	Iterable<Result<Input, Persisted>> persist(Iterable<Result<Input, Data>> iterable);
}