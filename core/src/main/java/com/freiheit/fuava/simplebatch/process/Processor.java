package com.freiheit.fuava.simplebatch.process;

import com.freiheit.fuava.simplebatch.result.Result;


public interface Processor<I, O> {
	/**
	 * Process the input into some output.
	 * 
	 * This could of course include fetching data from some source.
	 * 
	 * <b>Note</b> that implementations function <b>MUST NOT</b> 
	 * throw any Exceptions (no RuntimeExceptions, no Throwables etc)!
	 * 
	 * @return an Iterable over the processing results. Each Item of the input must have at least one matching item in the output.
	 */
	Iterable<Result<I, O>> process(Iterable<I> inputs);
	
}