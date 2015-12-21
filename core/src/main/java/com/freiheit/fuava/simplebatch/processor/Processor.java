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

public interface Processor<OriginalItem, Input, Output> {

    /**
     * Write the processing results. Note that you can not expect to only
     * receive successful results, be prepared for failed results.
     * 
     * <p>
     * <b>Important I:</b> Note that your implementation <b>MUST</b> return
     * exactly one instance of Result for each Result item of the input
     * iterable.
     * </p>
     * 
     * <p>
     * <b>Important II:</b> Further note, that your implementation <b>MUST
     * NOT</b> throw any Throwables (for example, no
     * {@link NullPointerException}s or other {@link RuntimeException}!), but
     * <b>MUST<b> return an iterable with a failed result instead.
     * </p>
     * 
     */
    Iterable<Result<OriginalItem, Output>> process( Iterable<Result<OriginalItem, Input>> iterable );

    default <D> ChainedProcessor<OriginalItem, Input, D> then( final Processor<OriginalItem, Output, D> g ) {
        return new ChainedProcessor<OriginalItem, Input, D>( this, g );
    }
}