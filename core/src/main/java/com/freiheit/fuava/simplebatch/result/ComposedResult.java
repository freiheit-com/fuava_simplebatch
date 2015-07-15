package com.freiheit.fuava.simplebatch.result;

import com.google.common.collect.Iterables;

public final class ComposedResult<A, B> {
	
	public static final <A, B> ComposedResult<A, B> of(Result<A, ?> orig) {
		return new ComposedResult<A, B>(orig);
	}
	
	private Result.Builder<A, B> builder;
	private Object intermediateValue;
	
	private ComposedResult(Result<A, ?> orig) {
		builder = Result.<A, B>builder()
				.withInput(orig.getInput())
				.withThrowables(orig.getThrowables())
				.withWarningMessages(orig.getWarningMessages())
				.withFailureMessages(orig.getFailureMessages());
		intermediateValue = orig.getOutput();
	}

	public Result<A, B> failed(String message) {
		return builder.withFailureMessage(message).failed();
	}
	
	/**
	 * If there are no results, fail. If there is one successful result, return success. Add warnings for any 
	 * values that exceed.
	 */
	public Result<A, B> compose(Iterable<? extends Result<?, B>> results){
		if (results == null || Iterables.isEmpty(results)) {
			return builder.withFailureMessage("No intermediate results found. Intermediate input was " + intermediateValue).failed();
		}
		Result<?, B> firstSuccess = Iterables.find(results, Result::isSuccess);
		for (Result<?, B> r : results) {
			// add everything that was accumulated in the composed results
			builder
			.withFailureMessages(r.getFailureMessages())
			.withWarningMessages(r.getWarningMessages())
			.withThrowables(r.getThrowables());
		}
		if (firstSuccess == null) {
			return builder.failed();
		}
		return builder.withOutput(firstSuccess.getOutput()).success();
	}
}