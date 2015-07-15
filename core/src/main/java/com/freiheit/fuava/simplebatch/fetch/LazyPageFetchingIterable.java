package com.freiheit.fuava.simplebatch.fetch;

import java.util.Iterator;

import com.freiheit.fuava.simplebatch.fetch.PageFetcher.PagingInput;
import com.freiheit.fuava.simplebatch.result.Result;

public class LazyPageFetchingIterable<T> implements Iterator<Result<PagingInput, T>> {
	private final PageFetcher<T> fetcher;

	private final int pageSize;
	private final PageFetchingSettings<T> settings;
	
	private int from;
	
	private Result<PagingInput, T> next;
	
	public LazyPageFetchingIterable(
			PageFetcher<T> fetcher, int initialFrom,
			int pageSize, PageFetchingSettings<T> settings
	) {
		this.fetcher = fetcher;
		this.from = initialFrom;
		this.pageSize = pageSize; 
		this.settings = settings;
		next = advance();
	}

	@Override
	public boolean hasNext() {
		return settings.hasNext(from, pageSize, next);
	}

	@Override
	public Result<PagingInput, T> next() {
		Result<PagingInput, T> v = next;
		next = advance();
		return v;
	}

	private Result<PagingInput, T> advance() {
		Result<PagingInput, T> v = fetcher.fetch(from, pageSize);
		from += pageSize;
		return v;
	}
}