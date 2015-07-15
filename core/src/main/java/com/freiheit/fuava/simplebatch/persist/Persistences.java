package com.freiheit.fuava.simplebatch.persist;

import com.freiheit.fuava.simplebatch.result.ComposedResult;
import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimaps;

public class Persistences {
	/*
	private static final class ComposedPersistence<A, B, C> implements Persistence<A, B, C> {
		private final Persistence<B, C> g;
		private final Persistence<A, B> f;
		
		public ComposedPersistence(Persistence<B, C> g, Persistence<A, B> f) {
			this.g = g;
			this.f = f;
		}

		@Override
		public Iterable<Result<A, C>> persist(Iterable<A> inputs) {
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
	*/
   /**
	 * Compose two processors.
	 * Note that the input of g will be a set of the successful output values from f.
	 * Also note that f must not return null outputs for successfully processed items!
	 */
	public static <A, B, C, D> Persistence<A, B, D> compose(Persistence<A, C, D> g, Persistence<A, B, C> f) {
		throw new UnsupportedOperationException();
		//return new ComposedPersistence<A, B, C, D>(g, f);
	}
	
}
