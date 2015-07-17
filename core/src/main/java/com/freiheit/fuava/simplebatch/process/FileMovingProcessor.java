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

/**
 * @author tim.lessner@freiheit.com
 */
class FileMovingProcessor extends AbstractSingleItemProcessor<File, File> {

	private final String toDir;

	public FileMovingProcessor( final String toDir ) {
		this.toDir = toDir;
	}

	@Override
	public Result<File, File> processItem(File toMove) {
		final File moveTo = new File( toDir + "/" + toMove.getName() );
		try {
		
			if ( toMove.renameTo( moveTo ) ) {
				return Result.success( moveTo, toMove ) ;
			} else {
				return Result.failed( moveTo, "Failed to move file"  ) ;
			}
		} catch ( final Throwable t ) {
			return Result.failed( moveTo, t ) ;
		}

	}
}