package com.freiheit.fuava.simplebatch.processor;

import java.io.InputStream;

import org.apache.http.client.HttpClient;

import com.freiheit.fuava.simplebatch.http.HttpDownloaderSettings;
import com.freiheit.fuava.simplebatch.http.HttpFetcher;
import com.freiheit.fuava.simplebatch.http.HttpFetcherImpl;
import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Function;

class HttpDownloader<Input, Id, T> extends AbstractSingleItemProcessor<Input, Id, T> {

    private final HttpFetcher fetcher;
    private final HttpDownloaderSettings<Id> settings;
    private final Function<InputStream, T> converter;

    public HttpDownloader(
            final HttpClient client,
            final HttpDownloaderSettings<Id> settings,
            final Function<InputStream, T> converter ) {
        this( new HttpFetcherImpl( client ), settings, converter );
    }

    public HttpDownloader(
            final HttpFetcher fetcher,
            final HttpDownloaderSettings<Id> settings,
            final Function<InputStream, T> converter ) {
        this.fetcher = fetcher;
        this.settings = settings;
        this.converter = converter;
    }

    @Override
    public Result<Input, T> processItem( final Result<Input, Id> data ) {
        if ( data.isFailed() ) {
            return Result.<Input, T> builder( data ).failed();
        }
        final Input input = data.getInput();
        final Id id = data.getOutput();
        try {
            final T result = fetcher.fetch( converter, settings.createFetchUrl( id ), settings.getRequestHeaders() );
            return Result.success( input, result );
        } catch ( final Throwable e ) {
            return Result.failed( input, e );
        }

    }
}