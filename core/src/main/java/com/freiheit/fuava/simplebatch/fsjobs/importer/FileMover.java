/**
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
