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

final class ComposedProcessor<OriginalItem, Input, Intermediate, Output> implements Processor<OriginalItem, Input, Output> {
    private final Processor<OriginalItem, Input, Intermediate> f;
    private final Processor<OriginalItem, Intermediate, Output> g;

    ComposedProcessor( final Processor<OriginalItem, Intermediate, Output> g, final Processor<OriginalItem, Input, Intermediate> f ) {
        this.g = g;
        this.f = f;
    }

    Processor<OriginalItem, Input, Intermediate> getFirst() {
        return f;
    }

    Processor<OriginalItem, Intermediate, Output> getSecond() {
        return g;
    }

    @Override
    public Iterable<Result<OriginalItem, Output>> process( final Iterable<Result<OriginalItem, Input>> toPersist ) {
        final Iterable<Result<OriginalItem, Intermediate>> fResults = this.f.process( toPersist );
        final Iterable<Result<OriginalItem, Output>> gResults = this.g.process( fResults );
        return gResults;
    }
    
    @Override
    public String getStageName() {
        return this.f.getStageName() + ", " + this.g.getStageName();
    }

}