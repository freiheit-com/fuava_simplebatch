package com.freiheit.fuava.simplebatch.processor;

import java.util.List;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class RetryingProcessorTest {
  @Test
  public void testNoFailures() {
       final List<Result<Integer, Integer>> result = execute(items -> items, 1, 2, 3);
       Assert.assertEquals( result.size(), 3 );
       Assert.assertTrue( result.stream().allMatch( Result::isSuccess ) );
       Assert.assertTrue( result.stream().noneMatch( Result::isFailed) );
       Assert.assertEquals( result.stream().filter( Result::isSuccess ).map( Result::getOutput).collect( Collectors.toSet() ), ImmutableSet.of( 1, 2, 3 ) );
  }

  @Test
  public void testFailBatchSucceedSingle() {
       final List<Result<Integer, Integer>> result = execute(items -> { if (items.size() > 1 ) throw new IllegalArgumentException(); return items; }, 1, 2, 3);
       Assert.assertEquals( result.size(), 3 );
       Assert.assertTrue( result.stream().allMatch( Result::isSuccess ) );
       Assert.assertTrue( result.stream().noneMatch( Result::isFailed) );
       Assert.assertEquals( result.stream().filter( Result::isSuccess ).map( Result::getOutput).collect( Collectors.toSet() ), ImmutableSet.of( 1, 2, 3 ) );
  }

  @Test
  public void testFailAll() {
       final List<Result<Integer, Integer>> result = execute(items -> {throw new IllegalArgumentException();}, 1, 2, 3);
       Assert.assertEquals( result.size(), 3 );
       Assert.assertTrue( result.stream().allMatch( Result::isFailed ) );
       Assert.assertTrue( result.stream().noneMatch( Result::isSuccess) );
       Assert.assertEquals( result.stream().filter( Result::isSuccess ).map( Result::getOutput).collect( Collectors.toSet() ), ImmutableSet.of() );
  }

  @Test
  public void testFailSome() {
       final List<Result<Integer, Integer>> result = execute(items -> {if (items.contains(2)) throw new IllegalArgumentException(); return items;}, 1, 2, 3);
       Assert.assertEquals( result.size(), 3 );
       Assert.assertTrue( result.stream().anyMatch( Result::isSuccess ) );
       Assert.assertTrue( result.stream().anyMatch( Result::isFailed) );
       Assert.assertEquals( result.stream().filter( Result::isSuccess ).map( Result::getOutput).collect( Collectors.toSet() ), ImmutableSet.of( 1, 3 ) );
  }

  private List<Result<Integer, Integer>> execute(final Function<List<Integer>, List<Integer>> func, final int...input) {
      return ImmutableList.copyOf( new RetryingFunctionProcessor<Integer, Integer, Integer>( func )
      .process( prepare( input ) ) );
  }
       
  private Iterable<Result<Integer, Integer>> prepare(final int... input) {
      final ImmutableList.Builder<Result<Integer, Integer>> b = ImmutableList.builder();
      for (int i = 0; i < input.length ; i++) {
          b.add( Result.success( input[i], input[i] ) );
      }
      return b.build();
  }
}
