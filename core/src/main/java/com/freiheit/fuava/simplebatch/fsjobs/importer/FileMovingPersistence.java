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

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.processor.AbstractSingleItemProcessor;
import com.freiheit.fuava.simplebatch.result.Result;

public class FileMovingPersistence<D> extends AbstractSingleItemProcessor<FetchedItem<ControlFile>, D, D> {

    private final FileMover fileMover = new FileMover();
    private final File processingDir;
    private final File archivedDir;
    private final File failedDir;

    public FileMovingPersistence( final String processingDir, final String archivedDir, final String failedDir ) {
        this( new File( processingDir ), new File( archivedDir ), new File( failedDir ) );
    }

    public FileMovingPersistence( final File processingDir, final File archivedDir, final File failedDir ) {
        super();
        this.processingDir = processingDir;
        this.archivedDir = archivedDir;
        this.failedDir = failedDir;
    }

    @Override
    public Result<FetchedItem<ControlFile>, D> processItem( final Result<FetchedItem<ControlFile>, D> r ) {
        final FetchedItem<ControlFile> input = r.getInput();
        final ControlFile controlFile = input.getValue();
        try {
            if ( r.isFailed() ) {
                moveBoth( controlFile, failedDir );
                // even though this operation was successful, we must not say success if the original item failed!
                return Result.<FetchedItem<ControlFile>, D> builder( r ).failed();

            } else {
                moveBoth( controlFile, archivedDir );
                return Result.success( input, r.getOutput() );
            }

        } catch ( final Throwable e ) {
            return Result.<FetchedItem<ControlFile>, D> builder( r ).failed( e );
        }
    }

    void moveBoth( final ControlFile input, final File targetDir ) throws FailedToMoveFileException {
        if ( input == null ) {
            throw new FailedToMoveFileException( "Cannot Move null control file." );
        }
        final File dir = processingDir;
        if ( !targetDir.exists() ) {
            if ( !targetDir.mkdirs() ) {
                throw new FailedToMoveFileException( "could not create directory " + processingDir );
            }
        }
        fileMover.moveFile( new File( dir, input.getFileName() ), targetDir );
        final File logFile = new File( dir, input.getLogFileName() );
        if ( logFile.exists() ) {
            fileMover.moveFile( logFile, targetDir );
        }
        if ( input.getControlledFileName() != null ) {
            fileMover.moveFile( new File( dir, input.getControlledFileName() ), targetDir );
        }
    }

}