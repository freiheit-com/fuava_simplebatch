package com.freiheit.fuava.simplebatch.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.http.HttpResponse;

import com.freiheit.fuava.simplebatch.exceptions.FetchFailedException;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

public final class Utils {

    private Utils() {
        // static util class constructor
    }

    public static String consumeAsString( HttpResponse response ) throws IOException, FetchFailedException {
        return consumeAsString( response.getEntity().getContent() );
    }

    public static String consumeAsString( InputStream istream ) throws FetchFailedException {
        try {
            try ( InputStream stream = istream ) {
                return CharStreams.toString( new InputStreamReader( stream, Charsets.UTF_8 ) );
            }
        } catch ( IOException e ) {
            throw new FetchFailedException( e );
        }
    }
}
