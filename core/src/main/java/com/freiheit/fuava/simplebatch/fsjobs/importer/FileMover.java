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

    public File moveFile( final File toMove, final String destinationDir ) throws FailedToMoveFileException {
        return moveFile( toMove, new File( destinationDir ) );
    }

    public File moveFile( final File toMove, final File destinationDir ) throws FailedToMoveFileException {
        final File moveTo = new File( destinationDir, toMove.getName() );
        final boolean succeeded = toMove.renameTo( moveTo );

        if ( succeeded ) {
            return moveTo;
        }

        throw new FailedToMoveFileException( toMove, moveTo );

    }
}
