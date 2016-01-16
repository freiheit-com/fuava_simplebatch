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
package com.freiheit.fuava.simplebatch.fetch;

import java.util.Iterator;

import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.util.EagernessUtil;
import com.google.common.collect.Iterators;

public final class IterableFetcherWrapper<T> implements Iterable<Result<FetchedItem<T>, T>> {
    private final Iterable<T> iterable;

    private IterableFetcherWrapper( final Iterable<T> iterable ) {
        this.iterable = iterable;
    }

    public static <T> Iterable<Result<FetchedItem<T>, T>> wrap (final Iterable<T> originalIterable) {
        return EagernessUtil.preserveEagerness( originalIterable, new IterableFetcherWrapper<T>( originalIterable ) );
    }

    @Override
    public Iterator<Result<FetchedItem<T>, T>> iterator() {
        try {
            return new FailsafeIterator<T>( iterable.iterator() );
        } catch ( final Throwable t ) {
            return Iterators.singletonIterator( Result.failed( null, t ) );
        }
    }

}