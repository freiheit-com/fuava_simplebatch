package com.freiheit.fuava.simplebatch.fetch;

import com.freiheit.fuava.simplebatch.fetch.PageFetcher.PagingInput;
import com.freiheit.fuava.simplebatch.result.Result;

public interface PageFetchingSettings<T> {
    boolean hasNext( int from, int amount, Result<PagingInput, T> lastValue );
}