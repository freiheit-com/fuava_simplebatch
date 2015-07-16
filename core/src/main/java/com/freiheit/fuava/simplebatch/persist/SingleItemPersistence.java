package com.freiheit.fuava.simplebatch.persist;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.collect.ImmutableList;

public abstract class SingleItemPersistence<Input, Output, P> implements Persistence<Input, Output, P> {
	
	@Override
	public final Iterable<Result<Input, P>> persist(Iterable<Result<Input, Output>> iterable) {
		ImmutableList.Builder<Result<Input, P>> b = ImmutableList.builder();
		for (Result<Input, Output> input: iterable) {
			b.add(persistItem(input));
		}
		return b.build();
	}
	
	public abstract Result<Input, P> persistItem(Result<Input, Output> input);
}