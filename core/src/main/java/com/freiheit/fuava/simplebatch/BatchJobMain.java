/**
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
package com.freiheit.fuava.simplebatch;

import java.io.PrintStream;

import com.freiheit.fuava.simplebatch.result.Counts;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;

/**
 * Executes a {@link BatchJob} as suitable for a command line tool, using
 * System.exit and System.out.println as applicable.
 *
 * @author klas
 */
public class BatchJobMain {

    private static void printCounts( final PrintStream out, final String type, final Counts counts ) {
        out.append( type )
                .append( ":\terrors = " )
                .append( Long.toString( counts.getError() ) )
                .append( ", success = " )
                .append( Long.toString( counts.getSuccess() ) )
                .append( '\n' );
    }

    public static <Input, Output> void exec( final BatchJob<Input, Output> batchJob ) {
        final long startTimeMillis = System.currentTimeMillis();
        final ResultStatistics statistics = batchJob.run();
        final long endTimeMillis = System.currentTimeMillis();

        final boolean allFailed = printStatistics( startTimeMillis, statistics, endTimeMillis );

        if ( allFailed ) {
            System.exit( -1 );
        }
        // will exit by itself
    }

    private static <Input, Output> boolean printStatistics(
            final long startTimeMillis,
            final ResultStatistics statistics, final long endTimeMillis ) {
        final Counts fetchCounts = statistics.getFetchCounts();

        final Counts processingCounts = statistics.getProcessingCounts();

        final boolean allSuccess = statistics.isAllSuccess();

        final boolean allFailed = statistics.isAllFailed();

        final PrintStream out = System.out;

        out.append( "-----------------------------------" ).append( '\n' );
        out.append( "  Status: " ).append( allSuccess
            ? "SUCCESS"
            : ( allFailed
                ? "FAIL"
                : "HAS ERRORS" ) ).append( '\n' );
        out.append( "-----------------------------------" ).append( '\n' );
        printCounts( out, "Fetch", fetchCounts );
        printCounts( out, "Processing", processingCounts );
        out.append( "Duration: " ).append( Long.toString( endTimeMillis - startTimeMillis ) ).append( " ms " ).append( '\n' );
        return allFailed;
    }
}
