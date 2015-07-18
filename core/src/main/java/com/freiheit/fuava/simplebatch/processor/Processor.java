package com.freiheit.fuava.simplebatch.processor;

import com.freiheit.fuava.simplebatch.result.Result;

public interface Processor<Input, Data, Persisted> {
	
	/**
	 * Write the processing results. Note that you can not expect to only receive successful results, be prepared for failed results.
	 */
	Iterable<Result<Input, Persisted>> process(Iterable<Result<Input, Data>> iterable);
}