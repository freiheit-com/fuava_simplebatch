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

import com.freiheit.fuava.simplebatch.exceptions.FetchFailedException;
import com.freiheit.fuava.simplebatch.http.HttpFetcher;
import com.freiheit.fuava.simplebatch.result.Result;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Test
public class HttpPagingFetcherTest {

    private static class DummyHttpFetcher implements HttpFetcher {

        @Override
        public <T> T fetch( final Function<? super InputStream, T> reader, final String uri, final Map<String, String> headers )
            throws FetchFailedException {
            return reader.apply( null );
        }

    }

    @Test
    public void testFetchLessThanExpected() {
        final DummyHttpFetcher httpFetcher = new DummyHttpFetcher();
        final List<String> fetchedList = Arrays.asList( "1", "2", "3", "vier" );
        final AtomicLong callCounter = new AtomicLong( 0 );
        final Fetcher<String> fetcher =
                Fetchers.httpPagingFetcher( httpFetcher,
                        ( from, amount ) -> "fetch?from=" + from + "&amount=" + amount,
                        arg0 -> {
                            // expected to be called twice
                            final long c = callCounter.incrementAndGet();
                            if ( c == 1 ) {
                                return fetchedList;
                            }
                            Assert.fail( "Should have been called exactly twice" );
                            return null;
                        },
                        0,
                        fetchedList.size() + 1
                        );

        final Iterable<Result<FetchedItem<String>, String>> all = fetcher.fetchAll();
        final List<String> result = StreamSupport.stream( all.spliterator(), false )
                .filter( Result::isSuccess )
                .map( Result::getOutput )
                .collect( Collectors.toList() );

        Assert.assertEquals( result, fetchedList );
        Assert.assertEquals( callCounter.get(), 1, "Expected exactly one call" );
    }

    @Test
    public void testFetchOnePageOfPageSize() {
        final DummyHttpFetcher httpFetcher = new DummyHttpFetcher();
        final List<String> fetchedList = Arrays.asList( "1", "2", "3", "vier" );
        final AtomicLong callCounter = new AtomicLong( 0 );
        final Fetcher<String> fetcher =
                Fetchers.httpPagingFetcher( httpFetcher,
                        ( from, amount ) -> "fetch?from=" + from + "&amount=" + amount,
                        arg0 -> {
                            // expected to be called twice
                            final long c = callCounter.incrementAndGet();
                            if ( c == 1 ) {
                                return fetchedList;
                            }
                            if ( c == 2 ) {
                                return Collections.emptyList();
                            }
                            Assert.fail( "Should have been called exactly twice" );
                            return null;
                        },
                        0,
                        fetchedList.size()
                        );

        final Iterable<Result<FetchedItem<String>, String>> all = fetcher.fetchAll();
        final List<String> result = StreamSupport.stream( all.spliterator(), false )
                .filter( Result::isSuccess )
                .map( Result::getOutput )
                .collect( Collectors.toList() );

        Assert.assertEquals( result, fetchedList );
        Assert.assertEquals( callCounter.get(), 2, "Expected exactly two calls" );
    }

    @Test
    public void testFetchTwoPages() {
        final DummyHttpFetcher httpFetcher = new DummyHttpFetcher();
        final List<String> fetchedList = Arrays.asList( "1", "2", "3", "vier" );
        final AtomicLong callCounter = new AtomicLong( 0 );
        final Fetcher<String> fetcher =
                Fetchers.httpPagingFetcher( httpFetcher,
                        ( from, amount ) -> "fetch?from=" + from + "&amount=" + amount,
                        arg0 -> {
                            // expected to be called twice
                            final long c = callCounter.incrementAndGet();
                            if ( c == 1 ) {
                                return fetchedList.subList( 0, fetchedList.size() - 1 );
                            }
                            if ( c == 2 ) {
                                return fetchedList.subList( fetchedList.size() - 1, fetchedList.size() );
                            }
                            Assert.fail( "Should have been called exactly twice" );
                            return null;
                        },
                        0,
                        fetchedList.size() - 1
                        );

        final Iterable<Result<FetchedItem<String>, String>> all = fetcher.fetchAll();
        final List<String> result = StreamSupport.stream( all.spliterator(), false )
                .filter( Result::isSuccess )
                .map( Result::getOutput )
                .collect( Collectors.toList() );

        Assert.assertEquals( result, fetchedList );
        Assert.assertEquals( callCounter.get(), 2, "Expected exactly two calls" );
    }
}
