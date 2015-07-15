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
import com.google.common.collect.ImmutableList;

/**
 * @author tim.lessner@freiheit.com
 */
public class PrepareControlledFileProcessor implements Processor<File, File> {

    private final String toDir;

    public PrepareControlledFileProcessor( final String toDir ) {
        this.toDir = toDir;
    }

    @Override
    public Iterable<Result<File, File>> process( final Iterable<File> inputs ) {
        final ImmutableList.Builder<Result<File, File>> builder = ImmutableList.<Result<File, File>> builder();
        for ( final File toMove : inputs ) {
            final File moveTo = new File( toDir + "/" + toMove.getName() );
            try {
                final boolean moved = toMove.renameTo( moveTo );

                if ( moved ) {
                    builder.add( Result.success( moveTo, toMove ) );
                } else {
                    builder.add( Result.failed( moveTo, new RuntimeException( "Failed to move file" ) ) );
                }
            } catch ( final Throwable t ) {
                builder.add( Result.failed( moveTo, t ) );
            }

        }
        return builder.build();
    }
}
