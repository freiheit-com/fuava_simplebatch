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
package com.freiheit.fuava.simplebatch.example;

import java.util.List;
import java.util.stream.Collectors;

import org.testng.annotations.Test;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fetch.Fetcher;
import com.freiheit.fuava.simplebatch.fetch.Fetchers;
import com.freiheit.fuava.simplebatch.processor.Processor;
import com.freiheit.fuava.simplebatch.processor.Processors;
import com.freiheit.fuava.simplebatch.result.Counts;
import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

@Test
public class SimpleLoopTest {

    @Test
    public void testLoop() {
        final Counts.Builder statistics = Counts.builder();

        final Fetcher<Integer> fetcher = Fetchers.iterable( ImmutableList.<Integer> of( 1, 2, 3, 4 ) );
        final Processor<FetchedItem<Integer>, Integer, Long> processor =
                Processors.retryableBatchedFunction( new Function<List<Integer>, List<Long>>() {

                    @Override
                    public List<Long> apply( final List<Integer> ids ) {
                        // Do interesting stuff, maybe using the ids to fetch the Article
                        // and then to store it
                        return ids.stream().map( id -> id.longValue() ).collect( Collectors.toList() );
                    }
                } );
        final Iterable<List<Result<FetchedItem<Integer>, Integer>>> partitions = Iterables.partition( fetcher.fetchAll(), 100 );
        for ( final List<Result<FetchedItem<Integer>, Integer>> sourceResults : partitions ) {
            statistics.addAll( processor.process( sourceResults ) );
        }

        final Counts counts = statistics.build();
        System.out.println( "Num Errors: " + counts.getError() );
        System.out.println( "Num Success: " + counts.getSuccess() );

    }
}
