package com.freiheit.fuava.simplebatch.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.freiheit.fuava.simplebatch.exceptions.FetchFailedException;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

/**
 * Utility functions for iostream handling within the fuava simplebatch project.
 * 
 * @author klas
 *
 */
public final class IOStreamUtils {

    private IOStreamUtils() {
        // static util class constructor
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
