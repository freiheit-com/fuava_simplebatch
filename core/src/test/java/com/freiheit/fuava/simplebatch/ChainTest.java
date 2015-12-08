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

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fetch.Fetchers;
import com.freiheit.fuava.simplebatch.processor.Processors;
import com.google.common.collect.ImmutableList;

/**
 * @author klas.kalass@freiheit.com
 */
@Test
public class ChainTest {

    @Test
    public void testChain() {
        final List<String> results = new ArrayList<String>();
        final BatchJob<Integer, String> job = BatchJob.<Integer, String> builder()
            .setFetcher(Fetchers.iterable( ImmutableList.of( 1, 2, 3, 4, 5 ) ) )
            .setProcessor(
                Processors.<FetchedItem<Integer>, Integer, Integer> singleItemFunction(input -> input.intValue() * 2 )
                .then( Processors.singleItemFunction(input -> "Wert: " + input ) )
                .then( Processors.singleItemFunction(input -> {results.add(input); return input;} ) )
            )
            .build();
        job.run();
        Assert.assertEquals( results, ImmutableList.of("Wert: 2", "Wert: 4", "Wert: 6", "Wert: 8", "Wert: 10") );
    }

}
