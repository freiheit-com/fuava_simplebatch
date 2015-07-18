/*
 * (c) Copyright 2015 freiheit.com technologies GmbH
 *
 * Created on 15.07.15 by tim.lessner@freiheit.com
 *
 * This file contains unpublished, proprietary trade secret information of
 * freiheit.com technologies GmbH. Use, transcription, duplication and
 * modification are strictly prohibited without prior written consent of
 * freiheit.com technologies GmbH.
 */

package com.freiheit.fuava.simplebatch;

import java.io.File;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.freiheit.fuava.simplebatch.processor.Processor;
import com.freiheit.fuava.simplebatch.processor.Processors;
import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * @author tim.lessner@freiheit.com
 */
public class TestPersistenceComposition {

    @Test
    public void testSimpleComposition() {
        final ImmutableList.Builder<Result<Integer, File>> builder = ImmutableList.<Result<Integer, File>> builder();
        builder.add( Result.success( 1, new File( "foo" ) ) );
        builder.add( Result.success( 2, new File( "foo" ) ) );
        final Processor<Integer, File, String> persistence1 = new Processor<Integer, File, String>() {
            @Override
            public Iterable<Result<Integer, String>> process( final Iterable<Result<Integer, File>> iterable ) {
                final ImmutableList.Builder<Result<Integer, String>> builder = ImmutableList.<Result<Integer, String>> builder();

                for ( final Result<Integer, File> toTransform : iterable ) {
                    if ( toTransform.getInput() == 2 ) {
                        builder.add( Result.success( toTransform.getInput(), toTransform.getOutput().getName() ) );

                    } else {
                        builder.add( Result.failed( toTransform.getInput(), toTransform.getOutput().getName() ) );

                    }
                }
                return builder.build();
            }
        };

        final Processor<Integer, String, Object> persistence2 = new Processor<Integer, String, Object>() {
            @Override
            public Iterable<Result<Integer, Object>> process( final Iterable<Result<Integer, String>> iterable ) {
                final ImmutableList.Builder<Result<Integer, Object>> builder = ImmutableList.<Result<Integer, Object>> builder();
                for ( final Result<Integer, String> toTransform : iterable ) {
                    if ( toTransform.isFailed() ) {
                        builder.add( Result.failed( toTransform.getInput(), new String( "hello" ) ) );
                    } else {
                        builder.add( Result.success( toTransform.getInput(), new String( "hello" ) ) );
                    }
                }
                return builder.build();

            }
        };

        final Processor<Integer, File, Object> compose = Processors.compose( persistence2, persistence1 );
        final Iterable<Result<Integer, Object>> persist = compose.process( builder.build() );

        final int sizeFailed = FluentIterable.from( persist ).filter( Result::isFailed ).toSet().size();
        final int sizeSuccess = FluentIterable.from( persist ).filter( Result::isSuccess ).toSet().size();
        Assert.assertEquals( sizeFailed, sizeSuccess, 1 );
    }
}
