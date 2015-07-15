package com.freiheit.fuava.simplebatch.fetch;

import java.util.Iterator;

import com.freiheit.fuava.simplebatch.result.Result;

public final class FailsafeIterable<T> implements Iterable<Result<?, T>> {
	private final Iterable<T> iterable;
	
	public FailsafeIterable(Iterable<T> iterable) {
		this.iterable = iterable;
	}
	
	@Override
	public Iterator<Result<?, T>> iterator() {
		return new FailsafeIterator<T>(iterable.iterator());
	}
	
}