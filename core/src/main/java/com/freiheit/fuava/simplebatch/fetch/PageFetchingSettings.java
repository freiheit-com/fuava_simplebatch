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

import com.freiheit.fuava.simplebatch.fetch.PageFetcher.PagingInput;
import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.collect.Iterables;

public interface PageFetchingSettings<T> {
    default boolean hasNext( final int from, final int amount, final Result<PagingInput, T> lastValue ) {
        if ( lastValue == null || !lastValue.isSuccess() ) {
            return false;
        }
        final Object output = lastValue.getOutput();
        if ( output instanceof Iterable ) {
            return Iterables.size( (Iterable) output ) >= amount;
        }
        throw new UnsupportedOperationException( "cannot calculate paging for " + output
                + " - please provide your own implementation" );

    }
}