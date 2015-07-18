package com.freiheit.fuava.simplebatch.processor;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Function;

public class SingleItemProcessor<Input, Output, PersistenceResult> extends AbstractSingleItemProcessor<Input, Output, PersistenceResult> {
    private final Function<Output, PersistenceResult> _func;

    public SingleItemProcessor(Function<Output, PersistenceResult> func) {
        _func = func;
    }
    @Override
    public Result<Input, PersistenceResult> processItem(Result<Input, Output> input) {
        if (!input.isSuccess()) {
            return Result.<Input, PersistenceResult>builder(input).failed();
        }
        Input ipt = input.getInput();
        try {
            return Result.success(ipt, _func.apply(input.getOutput()));
        } catch (Throwable t) {
            return Result.failed(ipt, t);
        }
    }

}
