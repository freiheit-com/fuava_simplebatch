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

import com.freiheit.fuava.simplebatch.result.Result;

import java.util.function.Function;

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
