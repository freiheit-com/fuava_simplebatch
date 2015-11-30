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
package com.freiheit.fuava.simplebatch.processor;

import java.io.File;

import com.freiheit.fuava.simplebatch.fsjobs.importer.ControlFile;
import com.freiheit.fuava.simplebatch.fsjobs.importer.FileMover;
import com.freiheit.fuava.simplebatch.result.Result;

/**
 * @author tim.lessner@freiheit.com
 * @author klas.kalass@freiheit.com
 */
class ControlledFileMovingProcessor<Input> extends AbstractSingleItemProcessor<Input, ControlFile, File> {
    private final FileMover fileMover = new FileMover();
    private final String processingDir;

    public ControlledFileMovingProcessor( final String processingDir ) {
        this.processingDir = processingDir;
    }

    @Override
    public Result<Input, File> processItem( final Result<Input, ControlFile> data ) {
        if ( data.isFailed() ) {
            // Creation of the control file faile, there is nothing we can do
            return Result.<Input, File> builder( data ).failed();
        }
        final Input input = data.getInput();
        final ControlFile ctl = data.getOutput();
        try {
            final File targetDir = new File( processingDir );
            if ( !targetDir.exists() ) {
                if ( !targetDir.mkdirs() ) {
                    throw new IllegalStateException( "Failed to create directory " + targetDir.getAbsolutePath() );
                }
            }
            //Move ctl file before processing. Do never work in the directory where all the data is written to!
            fileMover.moveFile( ctl.getFile(), processingDir );

            //move corresponding log file
            final File logFile = ctl.getLogFile();
            fileMover.moveFile( logFile, processingDir );

            //move file that is controlled by the control file
            if ( ctl.getControlledFileName() == null ) {
                return Result.failed( input, "No source file" );
            } else {
                final File controlledFile = ctl.getControlledFile();
                final File processingFile = fileMover.moveFile( controlledFile, processingDir );
                return Result.success( input, processingFile );
            }
        } catch ( final Throwable e ) {
            return Result.failed( input, e );
        }
    }
}
