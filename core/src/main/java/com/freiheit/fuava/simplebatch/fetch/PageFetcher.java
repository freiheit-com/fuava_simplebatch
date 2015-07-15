package com.freiheit.fuava.simplebatch.fetch;

import com.freiheit.fuava.simplebatch.result.Result;

public interface PageFetcher<T> {
	static final class PagingInput {
		public final int from;
		public final int pageSize;
		public PagingInput(int from, int pageSize) {
			this.from = from;
			this.pageSize = pageSize;
		}
	}
	
    Result<PagingInput, T> fetch(
    		int from, int pageSize
	);
}
