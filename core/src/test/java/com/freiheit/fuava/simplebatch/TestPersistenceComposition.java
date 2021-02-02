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
package com.freiheit.fuava.simplebatch;

import com.freiheit.fuava.simplebatch.processor.Processor;
import com.freiheit.fuava.simplebatch.processor.Processors;
import com.freiheit.fuava.simplebatch.result.Result;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author tim.lessner@freiheit.com
 */
public class TestPersistenceComposition {

    @Test
    public void testSimpleComposition() {
        final List<Result<Integer, File>> builder = Arrays.asList(
                Result.success( 1, new File( "foo" ) ),
                Result.success( 2, new File( "foo" ) ) );
        final Processor<Integer, File, String> persistence1 = iterable -> {
            final List<Result<Integer, String>> builder1 = new ArrayList<>();

            for ( final Result<Integer, File> toTransform : iterable ) {
                if ( toTransform.getInput() == 2 ) {
                    builder1.add( Result.success( toTransform.getInput(), toTransform.getOutput().getName() ) );
                } else {
                    builder1.add( Result.failed( toTransform.getInput(), toTransform.getOutput().getName() ) );
                }
            }
            return builder1;
        };

        final Processor<Integer, String, Object> persistence2 = iterable -> {
            final List<Result<Integer, Object>> builder12 = new ArrayList<>();
            for ( final Result<Integer, String> toTransform : iterable ) {
                if ( toTransform.isFailed() ) {
                    builder12.add( Result.failed( toTransform.getInput(), new String( "hello" ) ) );
                } else {
                    builder12.add( Result.success( toTransform.getInput(), new String( "hello" ) ) );
                }
            }
            return builder12;
        };

        final Processor<Integer, File, Object> compose = Processors.compose( persistence2, persistence1 );
        final Iterable<Result<Integer, Object>> persist = compose.process( builder );

        final int sizeFailed = StreamSupport.stream( persist.spliterator(), false )
                .filter( Result::isFailed )
                .collect( Collectors.toSet() )
                .size();
        final int sizeSuccess = StreamSupport.stream( persist.spliterator(), false )
                .filter( Result::isSuccess )
                .collect( Collectors.toSet() )
                .size();
        Assert.assertEquals( sizeFailed, sizeSuccess, 1 );
    }
}
