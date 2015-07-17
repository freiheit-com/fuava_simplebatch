package com.freiheit.fuava.simplebatch.persist;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Function;

public class SingleItemPersistence<Input, Output, PersistenceResult> extends AbstractSingleItemPersistence<Input, Output, PersistenceResult> {
	private final Function<Output, PersistenceResult> _func;
	
	public SingleItemPersistence(Function<Output, PersistenceResult> func) {
	   _func = func;
	}
	@Override
	public Result<Input, PersistenceResult> persistItem(Result<Input, Output> input) {
		Input ipt = input.getInput();
		try {
			return Result.success(ipt, _func.apply(input.getOutput()));
		} catch (Throwable t) {
			return Result.failed(ipt, t);
		}
	}

}
