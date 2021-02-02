/*******************************************************************************
 * Copyright (c) 2019 freiheit.com technologies gmbh
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
 *
 * @author: sami.emad@freiheit.com
 ******************************************************************************/

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

public class RetryingResultProcessorTest {
  @Test
  public void testNoFailures() {
       final List<Result<Integer, Integer>> result = execute( this::extractOutputs, 1, 2, 3);
       Assert.assertEquals( result.size(), 3 );
       Assert.assertTrue( result.stream().allMatch( Result::isSuccess ) );
       Assert.assertTrue( result.stream().noneMatch( Result::isFailed) );
       Assert.assertEquals( result.stream().filter( Result::isSuccess ).map( Result::getOutput).collect( Collectors.toSet() ), CollectionUtils.asSet( 1, 2, 3 ) );
  }

  @Test
  public void testFailBatchSucceedSingle() {
       final List<Result<Integer, Integer>> result = execute(items -> { if (items.size() > 1 ) throw new IllegalArgumentException(); return extractOutputs( items ); }, 1, 2, 3);
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
       final List<Result<Integer, Integer>> result = execute(items -> {if (items.stream().anyMatch(res -> res.getOutput()==2)) throw new IllegalArgumentException(); return extractOutputs( items );}, 1, 2, 3);
       Assert.assertEquals( result.size(), 3 );
       Assert.assertTrue( result.stream().anyMatch( Result::isSuccess ) );
       Assert.assertTrue( result.stream().anyMatch( Result::isFailed ) );
       Assert.assertEquals( result.stream().filter( Result::isSuccess ).map( Result::getOutput).collect( Collectors.toSet() ), CollectionUtils.asSet( 1, 3 ) );
  }

  private List<Result<Integer, Integer>> execute( final Function<List<Result<Integer, Integer>>, List<Integer>> func, final int...input) {
      return IterableUtils.asList( new RetryingResultFunctionProcessor<>( func ).process( prepare( input ) ) );
  }

  private Iterable<Result<Integer, Integer>> prepare(final int... input) {
      final List<Result<Integer, Integer>> b = new ArrayList<>();
      for ( int value : input ) {
          b.add( Result.success( value, value ) );
      }
      return b;
  }

  private List<Integer> extractOutputs( final List<Result<Integer, Integer>> items ) {
      return items.stream().map( Result::getOutput ).collect( Collectors.toList());
  }

}
