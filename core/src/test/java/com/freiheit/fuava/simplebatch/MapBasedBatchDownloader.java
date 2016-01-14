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
package com.freiheit.fuava.simplebatch;

import java.util.List;
import java.util.Map;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.processor.RetryingProcessor;
import com.google.common.collect.ImmutableList;

public final class MapBasedBatchDownloader<I, O> extends RetryingProcessor<FetchedItem<I>, I, O> {
    private final Map<I, O> map;

    public MapBasedBatchDownloader( final Map<I, O> map ) {
        this.map = map;
    }

    @Override
    public List<O> apply( final List<I> arg0 ) {
        final ImmutableList.Builder<O> b = ImmutableList.builder();
        for ( final I i : arg0 ) {
            b.add( map.get( i ) );
        }
        return b.build();
    }

}