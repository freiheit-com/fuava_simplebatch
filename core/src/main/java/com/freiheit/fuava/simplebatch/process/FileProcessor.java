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

package com.freiheit.fuava.simplebatch.process;

import java.io.File;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

/**
 * @author tim.lessner@freiheit.com
 */
public class FileProcessor<T> implements Processor<File, T> {

    private final Function<File, T> func;

    public FileProcessor( final Function<File, T> func ) {
        this.func = func;
    }

    @Override
    public Iterable<Result<File, T>> process( final Iterable<File> inputs ) {
        final ImmutableList.Builder<Result<File, T>> builder = ImmutableList.<Result<File, T>> builder();

        for ( final File toTransform : inputs ) {
            try {
                builder.add( Result.success( toTransform, func.apply( toTransform ) ) );
            } catch ( final Throwable t ) {
                builder.add( Result.failed( toTransform, t ) );
            }
        }

        return builder.build();
    }
}
