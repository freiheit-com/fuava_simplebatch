/**
 * Copyright 2015 freiheit.com technologies gmbh
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
 */
package com.freiheit.fuava.simplebatch.processor;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Function;

public class SingleItemProcessor<Input, Output, PersistenceResult> extends
        AbstractSingleItemProcessor<Input, Output, PersistenceResult> {
    private final Function<Output, PersistenceResult> _func;

    public SingleItemProcessor( final Function<Output, PersistenceResult> func ) {
        _func = func;
    }

    @Override
    public Result<Input, PersistenceResult> processItem( final Result<Input, Output> input ) {
        if ( !input.isSuccess() ) {
            return Result.<Input, PersistenceResult> builder( input ).failed();
        }
        final Input ipt = input.getInput();
        try {
            return Result.success( ipt, _func.apply( input.getOutput() ) );
        } catch ( final Throwable t ) {
            return Result.failed( ipt, t );
        }
    }

}
