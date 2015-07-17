package com.freiheit.fuava.simplebatch.process;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Function;

class SingleItemProcessor<Input, Output> extends AbstractSingleItemProcessor<Input, Output> {
	private final Function<Input, Output> func;
	
	public SingleItemProcessor(Function<Input, Output> func) {
		this.func = func;
	}
	
	public Result<Input, Output> processItem(Input input) {
		try {
			Output output = func.apply(input);
			return Result.success(input, output);
		} catch (Throwable t) {
			return Result.failed(input, t);
		}
	}
}