package com.freiheit.fuava.simplebatch.http;

import java.io.InputStream;
import java.util.Iterator;

import org.apache.http.client.HttpClient;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
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
    private final Function<? super InputStream, Iterable<T>> converter;
    private final int initialFrom;
    private final int pageSize;

    public HttpPagingFetcher(
            final HttpFetcher fetcher,
            final PagingRequestSettings<Iterable<T>> settings,
            final Function<? super InputStream, Iterable<T>> converter,
            final int initialFrom,
            final int pageSize ) {
        this.fetcher = fetcher;
        this.converter = converter;
        this.settings = settings;
        this.initialFrom = initialFrom;
        this.pageSize = pageSize;
    }

    public HttpPagingFetcher(
            final HttpClient client,
            final PagingRequestSettings<Iterable<T>> settings,
            final Function<InputStream, Iterable<T>> converter,
            final int initialFrom,
            final int pageSize ) {
        this( new HttpFetcherImpl( client ), settings, converter, initialFrom, pageSize );
    }

    private static final class ResultTransformer<T> implements
            Function<Result<PageFetcher.PagingInput, Iterable<T>>, Iterator<Result<FetchedItem<T>, T>>> {
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
                return Iterators.singletonIterator( Result.<FetchedItem<T>, T> builder( input, FetchedItem.of( null, counter++ ) ).failed() );
            }
            return Iterators.transform( input.getOutput().iterator(),
                    ( final T t ) -> Result.success( FetchedItem.of( t, counter++ ), t ) );
        }

    }

    @Override
    public Iterable<Result<FetchedItem<T>, T>> fetchAll() {
        return new Iterable<Result<FetchedItem<T>, T>>() {

            @Override
            public Iterator<Result<FetchedItem<T>, T>> iterator() {
                final Iterator<Result<PagingInput, Iterable<T>>> iterator = new LazyPageFetchingIterable<Iterable<T>>(
                        new HttpPageFetcher<Iterable<T>>( fetcher, settings, converter ),
                        initialFrom,
                        pageSize,
                        settings
                        );
                return Iterators.concat( Iterators.transform( iterator, new ResultTransformer<T>() ) );
            }

        };

    }
}