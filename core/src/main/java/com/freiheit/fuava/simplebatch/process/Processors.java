package com.freiheit.fuava.simplebatch.process;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimaps;

public class Processors {
	private static final class ComposedProcessor<A, B, C> implements Processor<A, C> {
		private final Processor<B, C> g;
		private final Processor<A, B> f;
		
		public ComposedProcessor(Processor<B, C> g, Processor<A, B> f) {
			this.g = g;
			this.f = f;
		}

		@Override
		public Iterable<Result<A, C>> process(Iterable<A> inputs) {
			//FIXME: try.. catch needed on a finer base - guard against exceptions triggered here.
			
			Iterable<Result<A, B>> fResults = this.f.process(inputs);
			
			Iterable<B> gInputs = FluentIterable.from(fResults).filter(Result::isSuccess).transform(Result::getOutput).toSet();
					
			Iterable<Result<B, C>> gResults = this.g.process(gInputs);
			
			ImmutableListMultimap<B, Result<B, C>> gResultsMap = Multimaps.index(gResults, Result::getInput);
			
			ImmutableList.Builder<Result<A, C>> b = ImmutableList.builder();
			for (Result<A, B> r: fResults) {
				b.add(composeResult(gResultsMap, r));
			}
			return b.build();
		}
		
		private Result<A, C> composeResult(
				ImmutableListMultimap<B, Result<B, C>> gResultsMap,
				Result<A, B> inputResult
		) {
			if (inputResult.isFailed()) {
				return ComposedResult.<A, C>of(inputResult).failed("Compose Step 1 aborted: " + f);
			} else {
				B intermediateInput = inputResult.getOutput();
				ImmutableList<Result<B, C>> intermediateResults = gResultsMap.get(intermediateInput);
				return ComposedResult.<A, C>of(inputResult).compose(intermediateResults);
			}
		}
		
		
	}
	
   static final class ComposedResult<A, B> {
		
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
   
	/**
	 * Compose two processors.
	 * Note that the input of g will be a set of the successful output values from f.
	 * Also note that f must not return null outputs for successfully processed items!
	 */
	public static <A, B, C> Processor<A, C> compose(Processor<B, C> g, Processor<A, B> f) {
		return new ComposedProcessor<A, B, C>(g, f);
	}
	
}
