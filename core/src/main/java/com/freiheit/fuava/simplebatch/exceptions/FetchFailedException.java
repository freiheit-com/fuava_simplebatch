package com.freiheit.fuava.simplebatch.exceptions;

public class FetchFailedException extends RuntimeException {

    public FetchFailedException( String msg ) {
        super( msg );
    }

    public FetchFailedException( Throwable t ) {
        super( t );
    }
}