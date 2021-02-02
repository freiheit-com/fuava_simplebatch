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
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import org.apache.http.client.HttpClient;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fetch.PageFetcher.PagingInput;
import com.freiheit.fuava.simplebatch.result.Result;

public class HttpPagingFetcher<T> extends ConvertingHttpPagingFetcher<T, T> {
    public HttpPagingFetcher(
            final Supplier<HttpClient> client,
            final PagingRequestSettings<Iterable<T>> settings,
            final Function<InputStream, Iterable<T>> converter,
            final int initialFrom,
            final int pageSize ) {
        super( client, settings, converter, new SimpleResultTransformerImpl<>(), initialFrom, pageSize );
    }
    
    public HttpPagingFetcher(
            final HttpFetcher fetcher,
            final PagingRequestSettings<Iterable<T>> settings,
            final Function<? super InputStream, Iterable<T>> converter,
                final int initialFrom,
                final int pageSize ) {
        super( fetcher, settings, converter, new SimpleResultTransformerImpl<>(), initialFrom, pageSize );
    }
    
    public HttpPagingFetcher(
            final HttpClient client,
            final PagingRequestSettings<Iterable<T>> settings,
            final Function<InputStream, Iterable<T>> converter,
            final int initialFrom,
            final int pageSize ) {
        super( client, settings, converter, new SimpleResultTransformerImpl<>(), initialFrom, pageSize );
    }

    
    private static final class SimpleResultTransformerImpl<T> implements ResultTransformer<T, T> {
        // ResultTransformer will be called while iterating over the
        // concatenated iterable. This will happen within one thread - so we do not
        // need to handle concurrency here. Furtheremore, we can simply use this function
        // to count the rows, because it is used tor transforming an iterator, not an iterable.
        private int counter;

        @Override
        public Iterator<Result<FetchedItem<T>, T>> apply( final Result<PagingInput, Iterable<T>> input ) {
            if ( input == null ) {
                return Collections.<Result<FetchedItem<T>, T>>singletonList(
                        Result.failed( FetchedItem.of( null, counter++ ),
                        "Transform called with null Input", null ) )
                        .iterator();
            }
            if ( input.isFailed() ) {
                return Collections.singletonList( Result.<FetchedItem<T>, T> builder( input, FetchedItem.of( null, counter++ ) ).failed() )
                        .iterator();
            }
            return StreamSupport.stream( input.getOutput().spliterator(), false )
                    .map( ( final T t ) -> Result.success( FetchedItem.of( t, counter++ ), t ) )
                    .iterator();
        }

    }

}