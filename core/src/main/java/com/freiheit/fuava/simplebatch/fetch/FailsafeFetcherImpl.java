package com.freiheit.fuava.simplebatch.fetch;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Supplier;

public final class FailsafeFetcherImpl<T> implements Fetcher<T>{
	private final Supplier<Iterable<T>> supplier;
	
	public FailsafeFetcherImpl(Supplier<Iterable<T>> supplier) {
		this.supplier = supplier;
	}
	
	@Override
	public Iterable<Result<?, T>> fetchAll() {
		return new FailsafeIterable<T>(this.supplier.get());
	}
	
}