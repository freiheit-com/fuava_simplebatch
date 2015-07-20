package com.freiheit.fuava.simplebatch.exceptions;

public class FetchFailedException extends RuntimeException {

    public FetchFailedException( final String msg ) {
        super( msg );
    }

    public FetchFailedException( final Throwable t ) {
        super( t );
    }
}