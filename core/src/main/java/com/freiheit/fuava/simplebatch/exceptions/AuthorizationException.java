package com.freiheit.fuava.simplebatch.exceptions;

public class AuthorizationException extends RuntimeException {
    public AuthorizationException( final String msg ) {
        super( msg );
    }
}