package com.freiheit.fuava.simplebatch.persist;

import java.util.List;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * A persistence implementation which delegates persistence of batches to a function.
 * 
 * If persisting of a batch failed, it will be divided into singleton batches and retried.
 * 
 * You have to ensure that aborting and retying the function will not lead to illegal states.
 * 
 * If your function persists to databases for example, you may need to ensure that your function opens
 * and closes the toplevel transaction and rolls back for <b>all</b> exceptions.
 * 
 * 
 * @param <Input>
 * @param <Output>
 */
class RetryingPersistence<Input, Output, PersistenceResult> implements Persistence<Input, Output, PersistenceResult> {
	private final Function<List<Output>, List<PersistenceResult>> _func;
	
	
	/**
	 * Creates a new processor that delegates to the given function.
	 * 
	 * Note that you need to ensure, that the input and output lists correspond to each other and that the 
	 * function supports retrying. For details, see the class documentation.
	 * 
	 * You have to ensure that your input and output lists have the same amount of 
	 * rows. The processor will assume that each position of input and output corresponds to 
	 * each other and will associate results accordingly.
	 * 
	 * Note that this function only gets the successfully processed Output values.
	 * If you need to persist all, you need to implement the Persistence interface yourself.
	 * @param func
	 */
	public RetryingPersistence(Function<List<Output>, List<PersistenceResult>> func) {
		_func = func;
	}
	
	@Override
	public Iterable<Result<Input, PersistenceResult>> persist(Iterable<Result<Input, Output>> inputs) {
		
		List<Result<Input, Output>> inputList = ImmutableList.copyOf(inputs);
		if (inputList.isEmpty()) {
			return ImmutableList.of();
		}
		try {
			return doPersist(inputList);
		} catch (Throwable t) {
			if (inputList.size() == 1) {
				Result<Input, Output> result = inputList.get(0);
				return ImmutableList.of(Result.<Input, PersistenceResult>builder(result).failed(t));
			}
			ImmutableList.Builder<Result<Input, PersistenceResult>> retriedResults = ImmutableList.builder();
			for (Result<Input, Output> input: inputList) {
				Iterable<Result<Input, PersistenceResult>> outputs = persist(ImmutableList.of(input));
				if (Iterables.isEmpty(outputs)) {
					throw new IllegalStateException("processing of singletons must never lead to empty lists here");
				}
				retriedResults.addAll(outputs);
			}
			return retriedResults.build();
		}
	}

	private Iterable<Result<Input, PersistenceResult>> doPersist(Iterable<Result<Input, Output>> iterable) {
		ImmutableList<Result<Input, Output>> successes = FluentIterable.from(iterable).filter(Result::isSuccess).toList();
		ImmutableList<Result<Input, Output>> fails = FluentIterable.from(iterable).filter(Result::isFailed).toList();
		
		ImmutableList<Output> outputs = FluentIterable.from(successes).transform(Result::getOutput).toList();
		
		List<PersistenceResult> persistenceResults = outputs.isEmpty() ? ImmutableList.of() : this._func.apply(outputs);
		
		if (persistenceResults.size() != outputs.size() || persistenceResults.size() != successes.size()) {
			throw new IllegalStateException("persistence results of unexpected size");
		}
		ImmutableList.Builder<Result<Input, PersistenceResult>> b = ImmutableList.builder();
		
		for (int i = 0; i < outputs.size(); i++) {
			Result<Input, Output> processingResult = successes.get(i);
			PersistenceResult persistenceResult = persistenceResults.get(i);
			b.add(Result.<Input, PersistenceResult>builder(processingResult).withOutput(persistenceResult).success());
		}
		
		for (Result<Input, Output> failed: fails) {
			b.add(Result.<Input, PersistenceResult>builder(failed).failed());
		}
		
		return b.build();
	}
}