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

package com.freiheit.fuava.simplebatch.fsjobs.importer;

import java.io.File;

import com.freiheit.fuava.simplebatch.process.Processor;
import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

/**
 * @author tim.lessner@freiheit.com
 */
public class ReadControlledFileProcessor<Id, T> implements Processor<ControlFile, T> {
    private final FileMover fileMover = new FileMover();
    private final String processingDir;
    private final String downloadDir;
    private final Function<File, ControlFile> transformFileToControlFile = new MakeControlFileFunction();
    private final Function<File, T> transformFileToResult;

    public ReadControlledFileProcessor( final String processingDir, final String downloadDir, final Function<File, T> transformFileToResult ) {
        this.processingDir = processingDir;
        this.downloadDir = downloadDir;
        this.transformFileToResult = transformFileToResult;
    }

    @Override
    public Iterable<Result<ControlFile, T>> process( final Iterable<ControlFile> inputs ) {
        final ImmutableList.Builder<Result<ControlFile, T>> builder = ImmutableList.<Result<ControlFile, T>> builder();
        for ( final ControlFile ctl : inputs ) {
            ControlFile controlFile = null;
            try {

                //Move ctl file before processing. Do never work in the directory where all the data is written to!
                final File processingCtlFile = fileMover.moveFile( ctl.getFile(), processingDir );
                controlFile = transformFileToControlFile.apply( processingCtlFile );

                //move file that is controlled by the control file
                final File controlledFile = new File( downloadDir + "/" + controlFile.getPathToControlledFile() );
                final File procFile = fileMover.moveFile( controlledFile, processingDir );
                final T result = transformFileToResult.apply( procFile );

                builder.add( Result.success( controlFile, result ) );
            } catch ( FailedToMoveFileException e ) {
                builder.add( Result.failed( controlFile, e ) );
            }
        }
        return builder.build();
    }
}
