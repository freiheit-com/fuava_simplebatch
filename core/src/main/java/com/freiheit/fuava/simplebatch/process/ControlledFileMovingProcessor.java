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

import com.freiheit.fuava.simplebatch.fsjobs.importer.ControlFile;
import com.freiheit.fuava.simplebatch.fsjobs.importer.FileMover;
import com.freiheit.fuava.simplebatch.result.Result;

/**
 * @author tim.lessner@freiheit.com
 * @author klas.kalass@freiheit.com
 */
class ControlledFileMovingProcessor extends AbstractSingleItemProcessor<ControlFile, File> {
    private final FileMover fileMover = new FileMover();
    private final String processingDir;
    
    public ControlledFileMovingProcessor( final String processingDir) {
        this.processingDir = processingDir;
    }

    @Override
    public Result<ControlFile, File> processItem(ControlFile ctl) {
		try {

		    //Move ctl file before processing. Do never work in the directory where all the data is written to!
			fileMover.moveFile( ctl.getFile(), processingDir );

		    //move file that is controlled by the control file
		    final File controlledFile = ctl.getControlledFile();
		    final File processingFile = fileMover.moveFile( controlledFile, processingDir );

		    return Result.success( ctl, processingFile ) ;
		} catch ( Throwable e ) {
		    return Result.failed( ctl, e ) ;
		}
	}
}
