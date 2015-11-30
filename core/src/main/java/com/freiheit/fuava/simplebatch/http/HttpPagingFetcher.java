/**
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
import java.util.Iterator;

import org.apache.http.client.HttpClient;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fetch.PageFetcher.PagingInput;
import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

public class HttpPagingFetcher<T> extends ConvertingHttpPagingFetcher<T, T> {

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
                return Iterators.singletonIterator( Result.failed( FetchedItem.of( null, counter++ ),
                        "Transform called with null Input", null ) );
            }
            if ( input.isFailed() ) {
                return Iterators.singletonIterator(
                        Result.<FetchedItem<T>, T> builder( input, FetchedItem.of( null, counter++ ) ).failed() );
            }
            return Iterators.transform( input.getOutput().iterator(),
                    ( final T t ) -> Result.success( FetchedItem.of( t, counter++ ), t ) );
        }

    }

}