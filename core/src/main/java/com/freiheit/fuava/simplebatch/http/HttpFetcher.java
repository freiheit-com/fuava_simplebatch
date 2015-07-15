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
import com.freiheit.fuava.simplebatch.util.Utils;
import com.google.common.base.Function;

public class HttpFetcher {
    static final Logger LOG = LoggerFactory.getLogger( HttpFetcher.class );
    private final HttpClient _client;

    public HttpFetcher( HttpClient client ) {
        _client = client;
    }

    public final <T> T fetch(
            Function<InputStream, T> reader,
            String uri,
            Map<String, String> headers
    ) throws FetchFailedException {
        try {
            final HttpGet get = new HttpGet( uri );
            for ( Map.Entry<String, String> e : headers.entrySet() ) {
                get.setHeader( e.getKey(), e.getValue() );
            }

            HttpResponse response = _client.execute( get );

            final int responseCode = response.getStatusLine().getStatusCode();

            if ( responseCode == 401 ) {
                throw new AuthorizationException( "Not allowed to access " + uri );
            }

            if ( !( responseCode == 200 || responseCode == 204 ) ) {
                // TODO: maybe we need to handle responses in a better way?
                throw new FetchFailedException(
                        String.format( "Failed to add articles for url %s. Response is %s ", uri, Utils.consumeAsString( response ) ) );
            }

            try ( InputStream stream = response.getEntity().getContent() ) {
                T result = reader.apply( stream );
                LOG.info( String.format( "transformed request result: %s", result ) );
                return  result ;
            }
        } catch ( IOException e ) {
            throw new FetchFailedException( e );
        }
    }

}