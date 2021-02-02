/*
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

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fetch.Fetcher;
import com.freiheit.fuava.simplebatch.fetch.Fetchers;
import com.freiheit.fuava.simplebatch.processor.Processor;
import com.freiheit.fuava.simplebatch.processor.Processors;
import com.freiheit.fuava.simplebatch.result.Counts;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.util.IterableUtils;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Test
public class SimpleLoopTest {

    @Test
    public void testLoop() {
        final Counts.Builder statistics = Counts.builder();

        final Fetcher<Integer> fetcher = Fetchers.iterable( Arrays.asList( 1, 2, 3, 4 ) );
        final Processor<FetchedItem<Integer>, Integer, Long> processor =
                Processors.retryableBatchedFunction( ids -> {
                    // Do interesting stuff, maybe using the ids to fetch the Article
                    // and then to store it
                    return ids.stream().map( Integer::longValue ).collect( Collectors.toList() );
                } );
        final Iterable<List<Result<FetchedItem<Integer>, Integer>>> partitions = IterableUtils.partition( fetcher.fetchAll(), 100 );
        for ( final List<Result<FetchedItem<Integer>, Integer>> sourceResults : partitions ) {
            statistics.addAll( processor.process( sourceResults ) );
        }

        final Counts counts = statistics.build();
        System.out.println( "Num Errors: " + counts.getError() );
        System.out.println( "Num Success: " + counts.getSuccess() );
    }
}
