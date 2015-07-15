package com.freiheit.fuava.simplebatch.http;

import java.io.InputStream;
import java.util.Iterator;

import org.apache.http.client.HttpClient;

import com.freiheit.fuava.simplebatch.fetch.Fetcher;
import com.freiheit.fuava.simplebatch.fetch.LazyPageFetchingIterable;
import com.freiheit.fuava.simplebatch.fetch.PageFetcher;
import com.freiheit.fuava.simplebatch.fetch.PageFetcher.PagingInput;
import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

public class HttpPagingFetcher<T> implements Fetcher<T> {
	
	private final HttpFetcher fetcher;
	private final PagingRequestSettings<Iterable<T>> settings;
	private final Function<InputStream, Iterable<T>> converter;
	private final int initialFrom;
	private final int pageSize;

	public HttpPagingFetcher(
			HttpClient client,
			PagingRequestSettings<Iterable<T>> settings,
			Function<InputStream, Iterable<T>> converter,
			int initialFrom, 
			int pageSize
	) {
		this.fetcher = new HttpFetcher(client);
		this.converter = converter;
		this.settings = settings;
		this.initialFrom = initialFrom;
		this.pageSize = pageSize;
	}

	private static final class ResultTransformer<T> implements Function<Result<PageFetcher.PagingInput, Iterable<T>>, Iterator<Result<?, T>>> {

		@Override
		public Iterator<Result<?, T>> apply(Result<PagingInput, Iterable<T>> input) {
			if (input == null) {
				return Iterators.singletonIterator(Result.failed(null,"Transform called with null Input", null));
			}
			if (input.isFailed()) {
				return Iterators.singletonIterator(Result.failed(input.getInput(), input.getFailureMessage(), input.getException()));
			}
			return Iterators.transform(input.getOutput().iterator(), (T t) -> Result.success(input.getInput(), t));
		}
		
	}
	public Iterable<Result<?, T>> fetchAll(
			
	) {
		return new Iterable<Result<?, T>>() {

			@Override
			public Iterator<Result<?, T>> iterator() {
				 Iterator<Result<PagingInput, Iterable<T>>> iterator = new LazyPageFetchingIterable<Iterable<T>>(
						new HttpPageFetcher<Iterable<T>>(fetcher, settings, converter), 
						initialFrom, 
						pageSize, 
						settings
				);
				return Iterators.concat(Iterators.transform(iterator, new ResultTransformer<T>()));
			}
			
		};
		
	}
}