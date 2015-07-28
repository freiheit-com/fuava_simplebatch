package com.freiheit.fuava.simplebatch.fetch;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.freiheit.fuava.simplebatch.exceptions.FetchFailedException;
import com.freiheit.fuava.simplebatch.http.HttpFetcher;
import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

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
        final ImmutableList<String> fetchedList = ImmutableList.of( "1", "2", "3", "vier" );
        final AtomicLong callCounter = new AtomicLong( 0 );
        final Fetcher<String> fetcher =
                Fetchers.<String> httpPagingFetcher( httpFetcher,
                        ( from, amount ) -> "fetch?from=" + from + "&amount=" + amount,
                        new Function<InputStream, Iterable<String>>() {

                            @Override
                            public Iterable<String> apply( final InputStream arg0 ) {
                                // expected to be called twice
                                final long c = callCounter.incrementAndGet();
                                if ( c == 1 ) {
                                    return fetchedList;
                                }
                                Assert.fail( "Should have been called exactly twice" );
                                return null;
                            }

                        },
                        0,
                        fetchedList.size() + 1
                        );

        final Iterable<Result<FetchedItem<String>, String>> all = fetcher.fetchAll();
        final ImmutableList<String> result =
                FluentIterable.from( all ).filter( Result::isSuccess ).transform( Result::getOutput ).toList();

        Assert.assertEquals( result, fetchedList );
        Assert.assertEquals( callCounter.get(), 1, "Expected exactly one call" );
    }

    @Test
    public void testFetchOnePageOfPageSize() {
        final DummyHttpFetcher httpFetcher = new DummyHttpFetcher();
        final ImmutableList<String> fetchedList = ImmutableList.of( "1", "2", "3", "vier" );
        final AtomicLong callCounter = new AtomicLong( 0 );
        final Fetcher<String> fetcher =
                Fetchers.<String> httpPagingFetcher( httpFetcher,
                        ( from, amount ) -> "fetch?from=" + from + "&amount=" + amount,
                        new Function<InputStream, Iterable<String>>() {

                            @Override
                            public Iterable<String> apply( final InputStream arg0 ) {
                                // expected to be called twice
                                final long c = callCounter.incrementAndGet();
                                if ( c == 1 ) {
                                    return fetchedList;
                                }
                                if ( c == 2 ) {
                                    return ImmutableList.of();
                                }
                                Assert.fail( "Should have been called exactly twice" );
                                return null;
                            }

                        },
                        0,
                        fetchedList.size()
                        );

        final Iterable<Result<FetchedItem<String>, String>> all = fetcher.fetchAll();
        final ImmutableList<String> result =
                FluentIterable.from( all ).filter( Result::isSuccess ).transform( Result::getOutput ).toList();

        Assert.assertEquals( result, fetchedList );
        Assert.assertEquals( callCounter.get(), 2, "Expected exactly two calls" );
    }

    @Test
    public void testFetchTwoPages() {
        final DummyHttpFetcher httpFetcher = new DummyHttpFetcher();
        final ImmutableList<String> fetchedList = ImmutableList.of( "1", "2", "3", "vier" );
        final AtomicLong callCounter = new AtomicLong( 0 );
        final Fetcher<String> fetcher =
                Fetchers.<String> httpPagingFetcher( httpFetcher,
                        ( from, amount ) -> "fetch?from=" + from + "&amount=" + amount,
                        new Function<InputStream, Iterable<String>>() {

                            @Override
                            public Iterable<String> apply( final InputStream arg0 ) {
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
                            }

                        },
                        0,
                        fetchedList.size() - 1
                        );

        final Iterable<Result<FetchedItem<String>, String>> all = fetcher.fetchAll();
        final ImmutableList<String> result =
                FluentIterable.from( all ).filter( Result::isSuccess ).transform( Result::getOutput ).toList();

        Assert.assertEquals( result, fetchedList );
        Assert.assertEquals( callCounter.get(), 2, "Expected exactly two calls" );
    }
}
