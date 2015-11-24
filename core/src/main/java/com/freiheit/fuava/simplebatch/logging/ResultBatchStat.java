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
    private final String description;

    public static String of( final Event event, final int failed, final int success, final String description ) {
        return new ResultBatchStat( event, failed, success, description ).toString();
    }

    public ResultBatchStat( final Event event, final int failed, final int success, final String description ) {
        this.event = event;
        this.failed = failed;
        this.success = success;
        this.description = description;
    }

    @Override
    public String toString() {
        final String summary = failed == 0
            ? "SUCCESS"
            : ( success == 0
                ? "FAILED"
                : "PARTIAL_SUCCESS" );
        return String.format( "%s | %s | success: %s | failed: %s | total: %s | %s ", event, summary, success, failed,
                success + failed, description );
    }
}
