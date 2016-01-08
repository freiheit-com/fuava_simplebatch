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

public abstract class AbstractSingleItemProcessor<OriginalItem, Input, Output> implements Processor<OriginalItem, Input, Output> {

    @Override
    public final Iterable<Result<OriginalItem, Output>> process( final Iterable<Result<OriginalItem, Input>> iterable ) {
        final ImmutableList.Builder<Result<OriginalItem, Output>> b = ImmutableList.builder();
        for ( final Result<OriginalItem, Input> input : iterable ) {
            b.add( processItem( input ) );
        }
        return b.build();
    }

    public abstract Result<OriginalItem, Output> processItem( Result<OriginalItem, Input> input );
}