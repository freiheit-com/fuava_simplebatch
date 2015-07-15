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
import com.google.common.collect.ImmutableList;

/**
 * @author tim.lessner@freiheit.com
 * @author klas.kalass@freiheit.com
 */
public class PrepareControlledFile implements Processor<ControlFile, File> {
    private final FileMover fileMover = new FileMover();
    private final String processingDir;
    private final String downloadDir;

    public PrepareControlledFile( final String processingDir, final String downloadDir) {
        this.processingDir = processingDir;
        this.downloadDir = downloadDir;
    }

    @Override
    public Iterable<Result<ControlFile, File>> process( final Iterable<ControlFile> inputs ) {
        final ImmutableList.Builder<Result<ControlFile, File>> builder = ImmutableList.<Result<ControlFile, File>>builder();
        for ( final ControlFile ctl : inputs ) {
            builder.add(process(ctl));
        }
        return builder.build();
    }

	private Result<ControlFile, File> process(final ControlFile ctl) {
		try {

		    //Move ctl file before processing. Do never work in the directory where all the data is written to!
			fileMover.moveFile( ctl.getFile(), processingDir );

		    //move file that is controlled by the control file
		    final File controlledFile = new File( downloadDir + "/" + ctl.getPathToControlledFile() );
		    final File processingFile = fileMover.moveFile( controlledFile, processingDir );

		    return Result.success( ctl, processingFile ) ;
		} catch ( Throwable e ) {
		    return Result.failed( ctl, e ) ;
		}
	}
}
