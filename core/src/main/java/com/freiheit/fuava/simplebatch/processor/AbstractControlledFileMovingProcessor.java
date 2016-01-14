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

import java.nio.file.Path;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fsjobs.importer.ControlFile;
import com.freiheit.fuava.simplebatch.result.Result;

/**
 * Moves the Controlled File (together with its control file and its processing log file) 
 * to a directory that reflects its processing result state (failed vs success). 
 * 
 * @author klas
 *
 * @param <Input>
 */
public abstract class AbstractControlledFileMovingProcessor<Input, Output> extends AbstractSingleItemProcessor<FetchedItem<ControlFile>, Input, Output> {

    private final Path sourceDir;
    private final Path archivedDir;
    private final Path failedDir;


    public AbstractControlledFileMovingProcessor( final Path sourceDir, final Path archivedDir, final Path failedDir ) {
        super();
        this.sourceDir = sourceDir;
        this.archivedDir = archivedDir;
        this.failedDir = failedDir;
    }

    @Override
    public Result<FetchedItem<ControlFile>, Output> processItem( final Result<FetchedItem<ControlFile>, Input> r ) {
        final FetchedItem<ControlFile> input = r.getInput();
        final ControlFile controlFile = input.getValue();
        try {
            if ( r.isFailed() ) {
                ControlFileMover.move( controlFile, this.sourceDir, failedDir );
                // even though this operation was successful, we must not say success if the original item failed!
                return Result.<FetchedItem<ControlFile>, Output> builder( r ).failed();

            } else {
                ControlFileMover.move( controlFile, this.sourceDir, archivedDir );
                return Result.success( input, getOutput(controlFile, r.getOutput()) );
            }

        } catch ( final Throwable e ) {
            return Result.<FetchedItem<ControlFile>, Output> builder( r ).failed( e );
        }
    }

    protected abstract Output getOutput( ControlFile controlFile, Input input );

}
