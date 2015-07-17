package com.freiheit.fuava.simplebatch.process;

import java.io.InputStream;

import org.apache.http.client.HttpClient;

import com.freiheit.fuava.simplebatch.http.HttpDownloaderSettings;
import com.freiheit.fuava.simplebatch.http.HttpFetcher;
import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Function;

class HttpDownloader<Id, T> extends AbstractSingleItemProcessor<Id, T> {

    private final HttpFetcher fetcher;
    private final HttpDownloaderSettings<Id> settings;
    private final Function<InputStream, T> converter;

    public HttpDownloader( 
    		final HttpClient client, 
    		final HttpDownloaderSettings<Id> settings,
    		final Function<InputStream, T> converter
	) {
        this.fetcher = new HttpFetcher( client );
        this.settings = settings;
        this.converter = converter;
    }

    @Override
    public Result<Id, T> processItem(Id id) {
        try {
            final T result = fetcher.fetch( converter, settings.createFetchUrl( id ), settings.getRequestHeaders() );
            return Result.success( id, result );
        } catch ( Throwable e ) {
            return Result.failed( id , e );
        }

    }
}