package com.freiheit.fuava.simplebatch.processor;

import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.util.CollectionUtils;
import com.freiheit.fuava.simplebatch.util.IterableUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RetryingProcessorTest {
  @Test
  public void testNoFailures() {
       final List<Result<Integer, Integer>> result = execute(items -> items, 1, 2, 3);
       Assert.assertEquals( result.size(), 3 );
       Assert.assertTrue( result.stream().allMatch( Result::isSuccess ) );
       Assert.assertTrue( result.stream().noneMatch( Result::isFailed) );
       Assert.assertEquals( result.stream().filter( Result::isSuccess ).map( Result::getOutput).collect( Collectors.toSet() ), CollectionUtils.asSet( 1, 2, 3 ) );
  }

  @Test
  public void testFailBatchSucceedSingle() {
       final List<Result<Integer, Integer>> result = execute(items -> { if (items.size() > 1 ) throw new IllegalArgumentException(); return items; }, 1, 2, 3);
       Assert.assertEquals( result.size(), 3 );
       Assert.assertTrue( result.stream().allMatch( Result::isSuccess ) );
       Assert.assertTrue( result.stream().noneMatch( Result::isFailed) );
       Assert.assertEquals( result.stream().filter( Result::isSuccess ).map( Result::getOutput).collect( Collectors.toSet() ), CollectionUtils.asSet( 1, 2, 3 ) );
  }

  @Test
  public void testFailAll() {
       final List<Result<Integer, Integer>> result = execute(items -> {throw new IllegalArgumentException();}, 1, 2, 3);
       Assert.assertEquals( result.size(), 3 );
       Assert.assertTrue( result.stream().allMatch( Result::isFailed ) );
       Assert.assertTrue( result.stream().noneMatch( Result::isSuccess) );
       Assert.assertEquals( result.stream().filter( Result::isSuccess ).map( Result::getOutput).collect( Collectors.toSet() ), Collections.emptySet() );
  }

  @Test
  public void testFailSome() {
       final List<Result<Integer, Integer>> result = execute(items -> {if (items.contains(2)) throw new IllegalArgumentException(); return items;}, 1, 2, 3);
       Assert.assertEquals( result.size(), 3 );
       Assert.assertTrue( result.stream().anyMatch( Result::isSuccess ) );
       Assert.assertTrue( result.stream().anyMatch( Result::isFailed) );
       Assert.assertEquals( result.stream().filter( Result::isSuccess ).map( Result::getOutput).collect( Collectors.toSet() ), CollectionUtils.asSet( 1, 3 ) );
  }

  private List<Result<Integer, Integer>> execute( final Function<List<Integer>, List<Integer>> func, final int...input) {
      return IterableUtils.asList( new RetryingFunctionProcessor<Integer, Integer, Integer>( func )
              .process( prepare( input ) ) );
  }
       
  private Iterable<Result<Integer, Integer>> prepare(final int... input) {
      final List<Result<Integer, Integer>> b = new ArrayList<>();
      for ( int value : input ) {
          b.add( Result.success( value, value ) );
      }
      return b;
  }
}
