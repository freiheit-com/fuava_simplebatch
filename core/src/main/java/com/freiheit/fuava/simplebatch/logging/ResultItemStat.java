/*
 * (c) Copyright 2015 freiheit.com technologies GmbH
 *
 * Created on 16.07.15 by tim.lessner@freiheit.com
 *
 * This file contains unpublished, proprietary trade secret information of
 * freiheit.com technologies GmbH. Use, transcription, duplication and
 * modification are strictly prohibited without prior written consent of
 * freiheit.com technologies GmbH.
 */

package com.freiheit.fuava.simplebatch.logging;

/**
 * Short statistics of processing one item. Contains the event, the messages (if
 * provided) and the input, e.g., an id of a document.
 * 
 * @author tim.lessner@freiheit.com
 */
public class ResultItemStat {

    private final Event event;
    private final Iterable<String> failureMessage;
    private final Iterable<Throwable> throwables;
    private final String input;

    private ResultItemStat( final Event event, Iterable<String> failureMessage, final Iterable<Throwable> throwables,
            final String input ) {
        this.event = event;
        this.failureMessage = failureMessage;
        this.throwables = throwables;
        this.input = input;
    }

    public static String formatted( final Event event, Iterable<String> failureMessage, final Iterable<Throwable> throwables,
            final String input ) {
        return new ResultItemStat( event, failureMessage, throwables, input ).format();
    }

    private String format() {
        final StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append( event );
        stringBuffer.append( " Input: " );
        stringBuffer.append( input );

        stringBuffer.append( " Failures: " );
        for ( final String s : failureMessage ) {
            stringBuffer.append( s );
        }

        stringBuffer.append( " Throwables: " );
        for ( final Throwable t : throwables ) {
            stringBuffer.append( t.getMessage() );
        }

        return stringBuffer.toString();
    }

    @Override
    public String toString() {
        return String.format( "%s failed. Input %s failure: %s", event, input, failureMessage );
    }

}
