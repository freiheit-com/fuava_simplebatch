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

public final class ChainedProcessor<A, B, D> implements Processor<A, B, D> {
    final Processor<A, B, D> f;
    private final String name;

    public <C> ChainedProcessor( final Processor<A, B, C> f, final Processor<A, C, D> g ) {
        name = f + " -> " + g;
        this.f = Processors.compose( g, f );
    }

    @Override
    public Iterable<Result<A, D>> process( final Iterable<Result<A, B>> iterable ) {
        return f.process( iterable );
    }

    @Override
    public String getStageName() {
        return this.f.getStageName();
    }
    
    @Override
    public String toString() {
        return name;
    }
}