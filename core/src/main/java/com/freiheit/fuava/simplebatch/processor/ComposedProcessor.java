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

final class ComposedProcessor<A, B, C, D> implements Processor<A, B, D> {
    private final Processor<A, B, C> f;
    private final Processor<A, C, D> g;

    ComposedProcessor( final Processor<A, C, D> g, final Processor<A, B, C> f ) {
        this.g = g;
        this.f = f;
    }

    @Override
    public Iterable<Result<A, D>> process( final Iterable<Result<A, B>> toPersist ) {
        final Iterable<Result<A, C>> fResults = this.f.process( toPersist );
        final Iterable<Result<A, D>> gResults = this.g.process( fResults );
        return gResults;
    }

}