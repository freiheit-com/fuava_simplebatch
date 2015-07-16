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
 * Short statistics of processing one batch. Contains at least the number of
 * successful and failed results. Also associates the event.
 * 
 * @author tim.lessner@freiheit.com
 */
public class ResultBatchStat {

    private final Event event;
    private final int failed;
    private final int success;

    public static String of( final Event event, final int failed, final int success ) {
        return new ResultBatchStat( event, failed, success ).toString();
    }

    public ResultBatchStat( final Event event, final int failed, final int success ) {
        this.event = event;
        this.failed = failed;
        this.success = success;
    }

    @Override
    public String toString() {
        return String.format( "%s failed: %s success: %s", event, failed, success );
    }
}
