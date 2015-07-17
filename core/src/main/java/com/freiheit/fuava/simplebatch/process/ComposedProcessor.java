package com.freiheit.fuava.simplebatch.process;

import com.freiheit.fuava.simplebatch.result.ComposedResult;
import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimaps;

final class ComposedProcessor<A, B, C> implements Processor<A, C> {
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