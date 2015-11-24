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
    public Result<Input, File> processItem( final Result<Input, File> data ) {
        if ( data.isFailed() ) {
            // The input (creation of the file) failed - no sense in trying to move it.
            return Result.<Input, File> builder( data ).failed();
        }
        final Input input = data.getInput();
        final File toMove = data.getOutput();
        final File moveTo = new File( toDir + "/" + toMove.getName() );
        try {

            if ( toMove.renameTo( moveTo ) ) {
                return Result.success( input, moveTo );
            } else {
                return Result.failed( input, "Failed to move file" );
            }
        } catch ( final Throwable t ) {
            return Result.failed( input, t );
        }

    }
}