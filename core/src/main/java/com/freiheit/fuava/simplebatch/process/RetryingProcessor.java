package com.freiheit.fuava.simplebatch.process;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * A processor which delegates processing of batches to a function.
 * 
 * If processing of a batch failed, it will be divided into singleton batches and retried.
 * 
 * You have to ensure, that aborting and retying the function will not lead to illegal states.
 * 
 * If your function persists to databases for example, you may need to ensure that your function open
 * and closes the toplevel transaction and rolls back for <b>all</b> exceptions.
 * 
 * 
 * @param <Input>
 * @param <Output>
 */
public class RetryingProcessor<Input, Output> implements Processor<Input, Output> {
	private final Function<List<Input>, Map<Input, Output>> _fkt;
	
	
	/**
	 * Creates a new processor that delegates to the given function.
	 * 
	 * Note that you need to ensure, that the input and output lists correspond to each other and that the 
	 * function supports retrying. For details, see the class documentation.
	 * 
	 * You have to ensure that your input and output lists have the same amount of 
	 * rows. The processor will assume that each position of input and output corresponds to 
	 * each other and will associate results accordingly.
	 * @param fkt
	 */
	public RetryingProcessor(Function<List<Input>, Map<Input, Output>> fkt) {
		_fkt = fkt;
	}
	
	@Override
	public Iterable<Result<Input, Output>> process(Iterable<Input> inputs) {
		List<Input> inputList = ImmutableList.copyOf(inputs);
		if (inputList.isEmpty()) {
			return ImmutableList.of();
		}
		try {
			return doProcess(inputList);
		} catch (Throwable t) {
			if (inputList.size() == 1) {
				return ImmutableList.of(Result.failed(inputList.get(0), t));
			}
			ImmutableList.Builder<Result<Input, Output>> retriedResults = ImmutableList.builder();
			for (Input input: inputList) {
				Iterable<Result<Input, Output>> outputs = process(ImmutableList.of(input));
				if (Iterables.isEmpty(outputs)) {
					throw new IllegalStateException("processing of singletons must never lead to empty lists here");
				}
				retriedResults.addAll(outputs);
			}
			return retriedResults.build();
		}
	}

	private Iterable<Result<Input, Output>> doProcess(List<Input> inputs) {

		Map<Input, Output> outputs = _fkt.apply(inputs);
		ImmutableList.Builder<Result<Input, Output>> rb = ImmutableList.builder();
		for (Input input : inputs) {
			if (!outputs.containsKey(input)) {
				rb.add(Result.failed(input, ""));
			} else {
				Output output = outputs.get(input);
				rb.add(Result.success(input, output));
			}    				
		}
		Set<Input> newInputs = Sets.difference(outputs.keySet(), ImmutableSet.copyOf(inputs));
		if (!newInputs.isEmpty()) {
			rb.add(Result.failed(null, newInputs.size() + " Unknown inputs returned by processor function: " + newInputs));
		}
		return rb.build();
	}
}