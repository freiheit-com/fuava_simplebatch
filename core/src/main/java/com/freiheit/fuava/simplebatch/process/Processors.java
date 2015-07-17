package com.freiheit.fuava.simplebatch.process;

import java.util.List;
import java.util.Map;

import com.google.common.base.Function;


public class Processors {
	/**
	 * Compose two processors.
	 * Note that the input of g will be a set of the successful output values from f.
	 * Also note that f must not return null outputs for successfully processed items!
	 */
	public static <A, B, C> Processor<A, C> compose(Processor<B, C> g, Processor<A, B> f) {
		return new ComposedProcessor<A, B, C>(g, f);
	}

	/**
	 * A Processor that does nothing.
	 */
	public static <A> Processor<A, A> identity() {
		return new IdentityProcessor<A>();
	}

	/**
	 * A Processor that processes a single item with the help of a function. 
	 * If it would be faster to perform the processing in batches (i. e. if you use databases), 
	 * use {@link #retryableBatch(Function)}.
	 * 
	 * @see #retryableBatch(Function)
	 */
	public static <Input, Output> Processor<Input, Output> single(Function<Input, Output> reader) {
		return new SingleItemProcessor<Input, Output>(reader);
	}

	/**
	 * A processor which delegates processing of batches to a function.
	 * 
	 * If processing of a batch failed, it will be divided into singleton batches and retried.
	 * 
	 * You have to ensure, that aborting and retying the function will not lead to illegal states.
	 * 
	 * If your function persists to databases for example, you may need to ensure that your function open
	 * and closes the toplevel transaction and rolls back for <b>all</b> exceptions.
	 * @param function
	 * @return
	 */
	public static <Input, Output> Processor<Input, Output> retryableBatch( 
			Function<List<Input>, List<Output>> function
	) {
		return new RetryingProcessor<Input, Output>(new MapBuildingFunction<Input, Output>(function));
	}

	/**
	 * Like {@link #retryableBatch(Function)}, but takes a function which produces a map from input to output.
	 */
	public static <Input, Output> Processor<Input, Output> retryableToMapBatch( 
			Function<List<Input>, Map<Input, Output>> function
	) {
		return new RetryingProcessor<Input, Output>(function);
	}
}
