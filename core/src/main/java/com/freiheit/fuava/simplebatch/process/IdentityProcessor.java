package com.freiheit.fuava.simplebatch.process;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.collect.ImmutableList;

public class IdentityProcessor<I> implements Processor<I, I> {

	@Override
	public Iterable<Result<I, I>> process(Iterable<I> inputs) {
		ImmutableList.Builder<Result<I, I>> b = ImmutableList.builder();
		for (I input: inputs) {
			b.add(Result.success(input, input));
		}
		return b.build();
	}

}
