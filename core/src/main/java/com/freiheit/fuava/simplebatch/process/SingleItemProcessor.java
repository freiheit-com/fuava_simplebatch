package com.freiheit.fuava.simplebatch.process;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.collect.ImmutableList;

public abstract class SingleItemProcessor<Input, Output> implements Processor<Input, Output> {
	
	
	@Override
	public final Iterable<Result<Input, Output>> process(Iterable<Input> inputs) {
		ImmutableList.Builder<Result<Input, Output>> b = ImmutableList.builder();
		for (Input input: inputs) {
			b.add(processItem(input));
		}
		return b.build();
	}
	
	public abstract Result<Input, Output> processItem(Input input);
}