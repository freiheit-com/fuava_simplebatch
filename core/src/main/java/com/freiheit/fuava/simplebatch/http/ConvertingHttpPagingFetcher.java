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

public class ConvertingHttpPagingFetcher<Raw, T> implements Fetcher<T> {

    private final HttpFetcher fetcher;
    private final PagingRequestSettings<Iterable<Raw>> settings;
    private final Function<? super InputStream, Iterable<Raw>> converter;
    private final int initialFrom;
    private final int pageSize;
    private final ResultTransformer<Raw, T> resultTransformer;

    public interface ResultTransformer<Raw, T> extends Function<Result<PageFetcher.PagingInput, Iterable<Raw>>, Iterator<Result<FetchedItem<T>, T>>>  {}

        
    public ConvertingHttpPagingFetcher(
            final HttpFetcher fetcher,
            final PagingRequestSettings<Iterable<Raw>> settings,
            final Function<? super InputStream, Iterable<Raw>> converter,
            final ResultTransformer<Raw, T> resultTransformer,
            final int initialFrom,
            final int pageSize ) {
        this.fetcher = fetcher;
        this.converter = converter;
        this.resultTransformer = resultTransformer;
        this.settings = settings;
        this.initialFrom = initialFrom;
        this.pageSize = pageSize;
    }

    public ConvertingHttpPagingFetcher(
            final HttpClient client,
            final PagingRequestSettings<Iterable<Raw>> settings,
            final Function<InputStream, Iterable<Raw>> converter,
            final ResultTransformer<Raw, T> resultTransformer,
            final int initialFrom,
            final int pageSize ) {
        this( new HttpFetcherImpl( client ), settings, converter, resultTransformer, initialFrom, pageSize );
    }

    public static final class ResultStateKeepingTransformerImpl<I, T> implements ResultTransformer<Result<I, T>, T> {
        // ResultTransformer will be called while iterating over the
        // concatenated iterable. This will happen within one thread - so we do not
        // need to handle concurrency here. Furtheremore, we can simply use this function
        // to count the rows, because it is used tor transforming an iterator, not an iterable.
        private int counter;

        @Override
        public Iterator<Result<FetchedItem<T>, T>> apply( final Result<PagingInput, Iterable<Result<I, T>>> input ) {
            if ( input == null ) {
                return Iterators.singletonIterator( Result.failed( FetchedItem.of( null, counter++ ),
                        "Transform called with null Input", null ) );
            }
            if ( input.isFailed() ) {
                return Iterators.singletonIterator( Result.<FetchedItem<T>, T> builder( input, FetchedItem.of( null, counter++ ) ).failed() );
            }
            final Iterator<Result<I, T>> fetchedItems = input.getOutput().iterator();
            return Iterators.transform( fetchedItems, new Function<Result<I, T>, Result<FetchedItem<T>, T>>() {
                @Override
                public Result<FetchedItem<T>, T> apply( final Result<I, T> input ) {
                    if (input.isFailed()) {
                        return Result.<FetchedItem<T>, T>builder()
                                .withInput( FetchedItem.of( null, counter++ ) )
                                .withFailureMessages( input.getFailureMessages() )
                                .withThrowables( input.getThrowables() )
                                .failed();
                        
                    }
                    return Result.<FetchedItem<T>, T>builder()
                            .withInput( FetchedItem.of( input.getOutput(), counter++ ) )
                            .success();
                }
            } );
        }

    }

    @Override
    public Iterable<Result<FetchedItem<T>, T>> fetchAll() {
        return new Iterable<Result<FetchedItem<T>, T>>() {

            @Override
            public Iterator<Result<FetchedItem<T>, T>> iterator() {
                final Iterator<Result<PagingInput, Iterable<Raw>>> iterator = new LazyPageFetchingIterable<Iterable<Raw>>(
                        new HttpPageFetcher<Iterable<Raw>>( fetcher, settings, converter ),
                        initialFrom,
                        pageSize,
                        settings
                        );
                return Iterators.concat( Iterators.transform( iterator, resultTransformer ) );
            }

        };

    }
}