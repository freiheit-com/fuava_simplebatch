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
package com.freiheit.fuava.simplebatch.http;

import java.io.InputStream;
import java.util.function.Function;

import com.freiheit.fuava.simplebatch.fetch.PageFetcher;
import com.freiheit.fuava.simplebatch.result.Result;

public class HttpPageFetcher<T> implements PageFetcher<T> {
    private final PagingRequestSettings<T> settings;
    private final HttpFetcher fetcher;
    private final Function<? super InputStream, T> converter;

    public HttpPageFetcher( final HttpFetcher fetcher, final PagingRequestSettings<T> settings,
            final Function<? super InputStream, T> converter ) {
        this.settings = settings;
        this.fetcher = fetcher;
        this.converter = converter;
    }

    @Override
    public Result<PagingInput, T> fetch( final int from, final int pageSize ) {
        final PagingInput pi = new PagingInput( from, pageSize );
        try {
            final T fetched = fetcher.fetch( converter, settings.createFetchUri( from, pageSize ), settings.getRequestHeaders() );
            return Result.success( pi, fetched );
        } catch ( final Throwable t ) {
            return Result.failed( pi, t );
        }
    }

}
