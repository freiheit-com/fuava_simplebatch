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
    private final String failureMessage;
    private final String input;

    public ResultItemStat( final Event event, String failureMessage, final String input ) {
        this.event = event;
        this.failureMessage = failureMessage;
        this.input = input;
    }

    public static String of( final Event event, final Iterable<String> failureMessage, final String input ) {
        return new ResultItemStat( event, prettyFailures( failureMessage ), input ).toString();
    }

    private static String prettyFailures( final Iterable<String> failureMessages ) {
        int i = 1;
        final StringBuffer stringBuffer = new StringBuffer();
        for ( final String s : failureMessages ) {
            stringBuffer.append( String.format( "Error %s", 1 ) );
            stringBuffer.append( s );
            i++;
        }
        return stringBuffer.toString();
    }

    @Override
    public String toString() {
        return String.format( "%s failed. Input %s failure: %s", event, input, failureMessage );
    }

}
