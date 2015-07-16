package com.freiheit.fuava.simplebatch;

import java.io.PrintStream;

import com.freiheit.fuava.simplebatch.result.ResultStatistics;
import com.freiheit.fuava.simplebatch.result.ResultStatistics.Counts;

/**
 * Executes a {@link BatchJob} as suitable for a command line tool, using System.exit and System.out.println as applicable.
 * 
 * @author klas
 */
public class BatchJobMain {

	
	private static boolean allFailed(Counts counts) {
		return counts.getError() != 0 && counts.getSuccess() == 0;
	}
	
	private static boolean allSuccess(Counts counts) {
		return counts.getError() == 0;
	}
	
	private static void printCounts(PrintStream out, String type, Counts counts) {
		out.append(type)
			.append(":\terrors = ")
			.append(Long.toString(counts.getError()))
			.append(", success = ")
			.append(Long.toString(counts.getSuccess()))
			.append('\n');
	}
	
	public static <Input, Output> void exec(BatchJob<Input, Output> batchJob) {
		final long startTimeMillis = System.currentTimeMillis();
		ResultStatistics<Input, Output> statistics = batchJob.run();
		final long endTimeMillis = System.currentTimeMillis();
				
		boolean allFailed = printStatistics(startTimeMillis, statistics, endTimeMillis);
		
		if (allFailed) {
			System.exit(-1);
		}
		// will exit by itself
	}

	private static <Input, Output> boolean printStatistics(
			final long startTimeMillis,
			ResultStatistics<Input, Output> statistics, final long endTimeMillis) {
		Counts fetchCounts = statistics.getFetchCounts();
		Counts processingCounts = statistics.getProcessingCounts();
		Counts persistCounts = statistics.getPersistCounts();
		
		boolean allSuccess = statistics.isAllSuccess();

		boolean allFailed = statistics.isAllFailed();

		PrintStream out = System.out;
		
		out.append("-----------------------------------").append('\n');
		out.append("  Status: ").append(allSuccess ? "SUCCESS" : (allFailed ? "FAIL" : "HAS ERRORS")).append('\n');
		out.append("-----------------------------------").append('\n');
		printCounts(out, "Fetch", fetchCounts);
		printCounts(out, "Processing", processingCounts);
		printCounts(out, "Persist", persistCounts);
		out.append("Duration: ").append(Long.toString(endTimeMillis - startTimeMillis)).append(" ms ").append('\n');
		return allFailed;
	}
}
