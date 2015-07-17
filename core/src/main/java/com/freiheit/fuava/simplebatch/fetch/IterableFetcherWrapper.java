package com.freiheit.fuava.simplebatch.fetch;

import java.util.Iterator;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.collect.Iterators;

public final class IterableFetcherWrapper<T> implements Iterable<Result<?, T>> {
	private final Iterable<T> iterable;
	
	public IterableFetcherWrapper(Iterable<T> iterable) {
		this.iterable = iterable;
	}
	
	@Override
	public Iterator<Result<?, T>> iterator() {
		try {
			return new FailsafeIterator<T>(iterable.iterator());
		} catch (Throwable t) {
			return Iterators.singletonIterator(Result.failed(null, t));
		}
	}
	
}