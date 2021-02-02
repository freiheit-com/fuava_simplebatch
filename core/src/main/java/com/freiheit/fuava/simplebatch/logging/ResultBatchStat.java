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
 * Short statistics of processing one batch. Contains at least the number of
 * successful and failed results. Also associates the event.
 * 
 * @author tim.lessner@freiheit.com
 */
public class ResultBatchStat {
    private final Event event;
    private final long failed;
    private final long success;
    private final String description;

    public static String of( final Event event, final long failed, final long success, final String description ) {
        return new ResultBatchStat( event, failed, success, description ).toString();
    }

    public ResultBatchStat( final Event event, final long failed, final long success, final String description ) {
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
