/*
 * (c) Copyright 2015 freiheit.com technologies GmbH
 *
 * Created on 14.07.15 by tim.lessner@freiheit.com
 *
 * This file contains unpublished, proprietary trade secret information of
 * freiheit.com technologies GmbH. Use, transcription, duplication and
 * modification are strictly prohibited without prior written consent of
 * freiheit.com technologies GmbH.
 */

package com.freiheit.fuava.simplebatch.fsjobs.importer;

import java.io.File;

import com.google.common.collect.ImmutableList;

/**
 * @author tim.lessner@freiheit.com
 */
public class FileMover {

    public Iterable<File> moveFiles( final Iterable<File> toMove, final String destination ) throws FailedToMoveFileException {
        final ImmutableList.Builder<File> builder = ImmutableList.<File> builder();
        for ( final File file : toMove ) {
            builder.add( moveFile( file, destination ) );
        }
        return builder.build();
    }

    public File moveFile( final File toMove, final String destination ) throws FailedToMoveFileException {
        final File moveTo = new File( destination + "/" + toMove.getName() );
        final boolean succeeded = toMove.renameTo( moveTo );

        if ( succeeded ) {
            return moveTo;
        }

        throw new FailedToMoveFileException( toMove.getAbsolutePath() );

    }
}
