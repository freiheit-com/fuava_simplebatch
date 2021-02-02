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
package com.freiheit.fuava.simplebatch.processor;

import java.io.InputStream;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.http.client.HttpClient;

import com.freiheit.fuava.simplebatch.http.HttpDownloaderSettings;
import com.freiheit.fuava.simplebatch.http.HttpFetcher;
import com.freiheit.fuava.simplebatch.http.HttpFetcherImpl;
import com.freiheit.fuava.simplebatch.result.Result;

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
            final Supplier<HttpClient> client,
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