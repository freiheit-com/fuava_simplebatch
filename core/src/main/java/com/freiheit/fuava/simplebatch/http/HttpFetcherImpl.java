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

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freiheit.fuava.simplebatch.exceptions.AuthorizationException;
import com.freiheit.fuava.simplebatch.exceptions.FetchFailedException;
import com.freiheit.fuava.simplebatch.util.IOStreamUtils;
import com.google.common.base.Function;

public class HttpFetcherImpl implements HttpFetcher {
    static final Logger LOG = LoggerFactory.getLogger( HttpFetcherImpl.class );
    private final HttpClient _client;

    public HttpFetcherImpl( final HttpClient client ) {
        _client = client;
    }

    @Override
    public final <T> T fetch(
            final Function<? super InputStream, T> reader,
            final String uri,
            final Map<String, String> headers
            ) throws FetchFailedException {
        try {
            final HttpGet get = new HttpGet( uri );
            for ( final Map.Entry<String, String> e : headers.entrySet() ) {
                get.setHeader( e.getKey(), e.getValue() );
            }

            final HttpResponse response = _client.execute( get );

            final int responseCode = response.getStatusLine().getStatusCode();

            if ( responseCode == 401 ) {
                throw new AuthorizationException( "Not allowed to access " + uri );
            }

            if ( !( responseCode == 200 || responseCode == 204 ) ) {
                // TODO: maybe we need to handle responses in a better way?
                throw new FetchFailedException(
                        String.format( "Failed to add articles for url %s. Response is %s ", uri,
                                IOStreamUtils.consumeAsString( response.getEntity().getContent() ) ) );
            }

            try ( InputStream stream = response.getEntity().getContent() ) {
                final T result = reader.apply( stream );
                LOG.debug( String.format( "transformed request result: %s", result ) );
                return result;
            }
        } catch ( final IOException e ) {
            throw new FetchFailedException( e );
        }
    }

}