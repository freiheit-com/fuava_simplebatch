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
import com.google.common.collect.ImmutableList;

public abstract class AbstractSingleItemProcessor<Input, Output, P> implements Processor<Input, Output, P> {

    @Override
    public final Iterable<Result<Input, P>> process( final Iterable<Result<Input, Output>> iterable ) {
        final ImmutableList.Builder<Result<Input, P>> b = ImmutableList.builder();
        for ( final Result<Input, Output> input : iterable ) {
            b.add( processItem( input ) );
        }
        return b.build();
    }

    public abstract Result<Input, P> processItem( Result<Input, Output> input );
}