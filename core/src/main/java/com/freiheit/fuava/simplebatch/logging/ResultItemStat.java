/*
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

    private ResultItemStat( final Event event, final Iterable<String> failureMessage, final Iterable<Throwable> throwables,
            final String input ) {
        this.event = event;
        this.failureMessage = failureMessage;
        this.throwables = throwables;
        this.input = input;
    }

    public static String formatted( final Event event, final Iterable<String> failureMessage, final Iterable<Throwable> throwables,
            final Object input ) {
        return new ResultItemStat( event, failureMessage, throwables, input == null
            ? null
            : input.toString() ).format();
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
