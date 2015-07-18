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

package com.freiheit.fuava.simplebatch.processor;

import java.io.File;

import com.freiheit.fuava.simplebatch.result.Result;

/**
 * @author tim.lessner@freiheit.com
 */
class FileMovingProcessor<Input> extends AbstractSingleItemProcessor<Input, File, File> {

    private final String toDir;

    public FileMovingProcessor( final String toDir ) {
        this.toDir = toDir;
    }

    @Override
    public Result<Input, File> processItem(Result<Input, File> data) {
        if (data.isFailed()) {
            // The input (creation of the file) failed - no sense in trying to move it.
            return Result.<Input, File>builder(data).failed();
        }
        Input input = data.getInput();
        File toMove = data.getOutput();
        final File moveTo = new File( toDir + "/" + toMove.getName() );
        try {

            if ( toMove.renameTo( moveTo ) ) {
                return Result.success( input, moveTo) ;
            } else {
                return Result.failed( input, "Failed to move file"  ) ;
            }
        } catch ( final Throwable t ) {
            return Result.failed( input, t ) ;
        }

    }
}