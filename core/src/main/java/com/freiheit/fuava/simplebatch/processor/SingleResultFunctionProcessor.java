/*
 * Copyright (c) 2019. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */
package com.freiheit.fuava.simplebatch.processor;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Function;

public class SingleResultFunctionProcessor<OriginalItem, Input, Output> extends
    SingleResultProcessor<OriginalItem, Input, Output> {

    private final Function<Result<OriginalItem, Input>, Output> _func;

    public SingleResultFunctionProcessor( final Function<Result<OriginalItem, Input>, Output> func ) {
        _func = func;
    }

    @Override
    protected Output apply( final Result<OriginalItem, Input> input ) {
        return _func.apply( input );
    }

    @Override
    public String getStageName() {
        return _func.getClass().getSimpleName();
    }

}
