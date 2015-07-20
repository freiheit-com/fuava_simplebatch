package com.freiheit.fuava.simplebatch.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.http.HttpResponse;

import com.freiheit.fuava.simplebatch.exceptions.FetchFailedException;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

/**
 * Utility functions for string handling within the fuava simplebatch project.
 * 
 * @author klas
 *
 */
public final class StringUtils {

    private StringUtils() {
        // static util class constructor
    }

    /**
     * Converts the value to a string, but only the first maxLenght characters.
     * Append '...' if there were more characters in the original string.
     */
    public static String toMaxLengthString( final Object value, final int maxLength ) {
        if ( value == null ) {
            return "null";
        }
        final String str = value.toString();

        final int length = str.length();

        return str.substring( 0, Math.min( 40, length ) ) + ( length > maxLength
            ? "..."
            : "" );
    }

    /**
     * Read the {@link HttpResponse}, consume its entity body and return it as a
     * string.
     */
    public static String consumeAsString( final HttpResponse response ) throws IOException, FetchFailedException {
        return consumeAsString( response.getEntity().getContent() );
    }

    /**
     * Read and close the given stream, returning the string. Note that the
     * stream is assumed to be UTF-8 encoded.
     * 
     */
    public static String consumeAsString( final InputStream istream ) throws FetchFailedException {
        try {
            try ( InputStream stream = istream ) {
                return CharStreams.toString( new InputStreamReader( stream, Charsets.UTF_8 ) );
            }
        } catch ( final IOException e ) {
            throw new FetchFailedException( e );
        }
    }
}
