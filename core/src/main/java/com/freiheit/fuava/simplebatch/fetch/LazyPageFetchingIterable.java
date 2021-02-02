/*
 * Copyright 2015 freiheit.com technologies gmbh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
    private boolean hasNext;

    public LazyPageFetchingIterable(
            final PageFetcher<T> fetcher, final int initialFrom,
            final int pageSize, final PageFetchingSettings<T> settings ) {
        this.fetcher = fetcher;
        this.from = initialFrom;
        this.pageSize = pageSize;
        this.settings = settings;
        advance();
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public Result<PagingInput, T> next() {
        final Result<PagingInput, T> v = next;
        // let the settings control wether or not we expect to see more results and thus need another page call
        if ( !settings.hasNext( from, pageSize, next ) ) {
            hasNext = false;
            next = null;
        } else {
            advance();
        }
        return v;
    }

    private void advance() {
        final Result<PagingInput, T> v = fetcher.fetch( from, pageSize );
        from += pageSize;
        hasNext = v != null;
        next = v;
    }
}