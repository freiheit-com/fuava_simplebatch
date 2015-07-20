package com.freiheit.fuava.simplebatch.fetch;

import com.freiheit.fuava.simplebatch.result.Result;

public interface PageFetcher<T> {
    public static final class PagingInput {
        public final int from;
        public final int pageSize;

        public PagingInput( final int from, final int pageSize ) {
            this.from = from;
            this.pageSize = pageSize;
        }
    }

    public Result<PagingInput, T> fetch(
            int from, int pageSize
            );
}
