package com.freiheit.fuava.simplebatch.http;

import java.io.InputStream;

import org.apache.http.client.HttpClient;

import com.freiheit.fuava.simplebatch.process.Processor;
import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

public class HttpByInputsFetcher<Id, T> implements Processor<Id, T> {

    private final HttpFetcher fetcher;
    private final ByIdRequestSettings<Id> settings;
    private final Function<InputStream, T> converter;

    public HttpByInputsFetcher( 
    		final HttpClient client, 
    		final ByIdRequestSettings<Id> settings,
    		final Function<InputStream, T> converter
	) {
        this.fetcher = new HttpFetcher( client );
        this.settings = settings;
        this.converter = converter;
    }

    public  Iterable<Result<Id, T>> process(
            Iterable<Id> ids
    ) {
        // FIXME: support real  batched fetching
        ImmutableList.Builder<Result<Id, T>> b = ImmutableList.builder();
        for ( Id id : ids ) {
            b.add( fetch( converter, id ) );
        }
        return b.build();
    }

    public Result<Id, T> fetch(
            Function<InputStream, T> converter,
            Id id
            ) {
        try {
            final T result = fetcher.fetch( converter, settings.createFetchUrl( id ), settings.getRequestHeaders() );
            return Result.success( id, result );
        } catch ( Throwable e ) {
            return Result.failed( id , e );
        }

    }
}