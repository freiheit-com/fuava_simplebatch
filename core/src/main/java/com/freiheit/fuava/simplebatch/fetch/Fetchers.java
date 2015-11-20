package com.freiheit.fuava.simplebatch.fetch;

import java.io.File;
import java.io.InputStream;
import java.util.Map;

import org.apache.http.client.HttpClient;

import com.freiheit.fuava.simplebatch.http.HttpFetcher;
import com.freiheit.fuava.simplebatch.http.HttpFetcherImpl;
import com.freiheit.fuava.simplebatch.http.HttpPagingFetcher;
import com.freiheit.fuava.simplebatch.http.PagingRequestSettings;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Ordering;

public class Fetchers {

    /**
     * Uses the given iterable to provide the data that will be passed on to the
     * processing stage, treating each item as successful.
     */
    public static final <T> Fetcher<T> iterable( final Iterable<T> iterable ) {
        return new SuppliedIterableFetcher<T>( Suppliers.ofInstance( iterable ) );
    }

    /**
     * Uses the given fetcher to get an iterable that provides the data that
     * will be passed on to the processing stage, treating each item as
     * successful.
     */
    public static final <T> Fetcher<T> supplied( final Supplier<Iterable<T>> supplier ) {
        return new SuppliedIterableFetcher<T>( supplier );
    }

    /**
     * A Fetcher that uses an http request to retrieve the data to process.
     */
    public static <Item> Fetcher<Item> httpFetcher(
            final HttpClient client,
            final String uri,
            final Map<String, String> headers,
            final Function<InputStream, Iterable<Item>> converter
            ) {
        return httpFetcher( new HttpFetcherImpl( client ), uri, headers, converter );
    }

    public static <Item> Fetcher<Item> httpFetcher(
            final HttpFetcher httpFetcher,
            final String uri,
            final Map<String, String> headers,
            final Function<InputStream, Iterable<Item>> converter
            ) {
        return new SuppliedIterableFetcher<Item>( new Supplier<Iterable<Item>>() {

            @Override
            public Iterable<Item> get() {
                return httpFetcher.fetch( converter, uri, headers );
            }
        } );
    }

    /**
     * Creates a Fetcher which fetches the Items lazily via http, always
     * requesting a page of the data and transparently continuing to the next
     * page.
     */
    public static <Item> Fetcher<Item> httpPagingFetcher(
            final HttpClient client,
            final PagingRequestSettings<Iterable<Item>> settings,
            final Function<? super InputStream, Iterable<Item>> converter,
            final int initialFrom,
            final int pageSize
            ) {
        return httpPagingFetcher( new HttpFetcherImpl( client ), settings, converter, initialFrom, pageSize );

    }

    /**
     * Creates a Fetcher which fetches the Items lazily via http, always
     * requesting a page of the data and transparently continuing to the next
     * page.
     */
    public static <Item> Fetcher<Item> httpPagingFetcher(
            final HttpFetcher fetcher,
            final PagingRequestSettings<Iterable<Item>> settings,
            final Function<? super InputStream, Iterable<Item>> converter,
            final int initialFrom,
            final int pageSize
            ) {
        return new HttpPagingFetcher<Item>( fetcher, settings, converter, initialFrom, pageSize );

    }

    /**
     * Iterates over all files in the given directory that end with the
     * specified fileEnding and converts them with the given function.
     */
    public static <T> Fetcher<T> folderFetcher( final String dirName, final String fileEnding, final Function<File, T> fileFunction ) {
        return folderFetcher( dirName, fileEnding, fileFunction, DirectoryFileFetcher.ORDERING_FILE_BY_PATH );
    }

    /**
     * Iterates over all files in the given directory that end with the
     * specified fileEnding.
     */
    public static Fetcher<File> folderFetcher( final String dirName, final String fileEnding ) {
        return folderFetcher( dirName, fileEnding, DirectoryFileFetcher.ORDERING_FILE_BY_PATH );
    }

    /**
     * Iterates over all files in the given directory that end with the
     * specified fileEnding and converts them with the given function.
     */
    public static <T> Fetcher<T> folderFetcher( final String dirName, final String fileEnding, final Function<File, T> fileFunction,
            final Ordering<File> fileOrdering ) {
        return new DirectoryFileFetcher<T>( dirName, fileEnding, fileFunction, fileOrdering );
    }

    /**
     * Iterates over all files in the given directory that end with the
     * specified fileEnding.
     */
    public static Fetcher<File> folderFetcher( final String dirName, final String fileEnding, final Ordering<File> fileOrdering ) {
        return new DirectoryFileFetcher<File>( dirName, fileEnding, Functions.<File> identity(), fileOrdering );
    }

}
