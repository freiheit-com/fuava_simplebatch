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
    public  void testLoop() {
        final Counts.Builder statistics = Counts.builder();

        final Fetcher<Integer> fetcher = Fetchers.iterable(ImmutableList.<Integer>of(1, 2, 3, 4));
        final Processor<FetchedItem<Integer>, Integer, Long> processor =
                Processors.retryableBatchedFunction(new Function<List<Integer>, List<Long>>() {

                    @Override
                    public List<Long> apply(List<Integer> ids) {
                        // Do interesting stuff, maybe using the ids to fetch the Article
                        // and then to store it
                        return ids.stream().map(id -> id.longValue()).collect(Collectors.toList());
                    }
                });
        Iterable<List<Result<FetchedItem<Integer>, Integer>>> partitions = Iterables.partition( fetcher.fetchAll(), 100 ) ;
        for ( List<Result<FetchedItem<Integer>, Integer>> sourceResults : partitions) {
            statistics.addAll(processor.process( sourceResults ));
        }

        Counts counts = statistics.build();
        System.out.println("Num Errors: " + counts.getError());
        System.out.println("Num Success: " + counts.getSuccess());

    }
}
