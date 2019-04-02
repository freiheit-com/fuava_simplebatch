/*******************************************************************************
 * Copyright (c) 2019 freiheit.com technologies gmbh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * @author: sami.emad@freiheit.com
 ******************************************************************************/

package com.freiheit.fuava.simplebatch.processor;

import java.util.List;
import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Function;

/**
 * A processor implementation which delegates processing of lists of
 * (successful) values to a function.
 *
 * If persisting of a batch failed, it will be divided into singleton batches
 * and retried.
 *
 * You have to ensure that aborting and retrying the function will not lead to
 * illegal states.
 *
 * If your function persists to databases for example, you may need to ensure
 * that your function opens and closes the toplevel transaction and rolls back
 * for <b>all</b> exceptions.
 *
 *
 * @param <OriginalItem>
 * @param <Input>
 */
class RetryingResultFunctionProcessor<OriginalItem, Input, Output> extends RetryingResultProcessor<OriginalItem, Input, Output> {
    private final Function<List<Result<OriginalItem, Input>>, List<Output>> _func;

    /**
     * Creates a new processor that delegates to the given function.
     *
     * Note that you need to ensure, that the input and output lists correspond
     * to each other and that the function supports retrying. For details, see
     * the class documentation.
     *
     * You have to ensure that your input and output lists have the same amount
     * of rows. The processor will assume that each position of input and output
     * corresponds to each other and will associate results accordingly.
     *
     * Note that this function only gets the successfully processed Output
     * values. If you need to persist all, you need to implement the Persistence
     * interface yourself.
     *
     * @param func
     */
    public RetryingResultFunctionProcessor( final Function<List<Result<OriginalItem, Input>>, List<Output>> func ) {
        _func = func;
    }

    @Override
    protected List<Output> apply( final List<Result<OriginalItem, Input>> input ) {
        return _func.apply( input );
    }
}