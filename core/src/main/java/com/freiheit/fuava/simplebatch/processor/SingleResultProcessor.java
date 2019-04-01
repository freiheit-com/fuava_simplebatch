/*
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

public abstract class SingleResultProcessor<OriginalItem, Input, Output> extends
        AbstractSingleItemProcessor<OriginalItem, Input, Output> {

    public SingleResultProcessor() {
    }

    protected abstract Output apply( Result<OriginalItem, Input> input );

    @Override
    public Result<OriginalItem, Output> processItem( final Result<OriginalItem, Input> input ) {
        if ( !input.isSuccess() ) {
            return Result.<OriginalItem, Output> builder( input ).failed();
        }
        final OriginalItem ipt = input.getInput();
        try {
            // The original item should not be changed by the implementation. That's why apply only returns the Output
            return Result.success( ipt, apply( input ) );
        } catch ( final VirtualMachineError e ) {
            // there is absolutely no way how those types of errors could be handled, rethrow it
            throw e;
        } catch ( final Throwable t ) {
            return Result.failed( ipt, t );
        }
    }

}
