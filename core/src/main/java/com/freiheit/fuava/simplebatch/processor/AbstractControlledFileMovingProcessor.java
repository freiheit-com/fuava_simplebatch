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
import com.freiheit.fuava.simplebatch.result.Result.Builder;

/**
 * Moves the Controlled File (together with its control file and its processing log file) 
 * to a directory that reflects its processing result state (failed vs success). 
 * 
 * @author klas
 *
 * @param <Input>
 */
public abstract class AbstractControlledFileMovingProcessor<Input, Output> extends AbstractSingleItemProcessor<FetchedItem<ControlFile>, Input, Output> {
    private final Path archivedDir;
    private final Path failedDir;


    public AbstractControlledFileMovingProcessor( final Path archivedDir, final Path failedDir ) {
        super();
        this.archivedDir = archivedDir;
        this.failedDir = failedDir;
    }

    @Override
    public Result<FetchedItem<ControlFile>, Output> processItem( final Result<FetchedItem<ControlFile>, Input> r ) {
        final Builder<FetchedItem<ControlFile>, Output> result = Result.<FetchedItem<ControlFile>, Output> builder( r );
        try {
            final FetchedItem<ControlFile> input = r.getInput();
            final ControlFile controlFile = input == null ? null : input.getValue();
            if ( controlFile == null ) {
                return result.withFailureMessage( "Cannot move entry with null Control File" ).failed();
            }
            
            final ControlFile targetControlFile = r.isFailed() ? controlFile.withBaseDir( failedDir ) : controlFile.withBaseDir( archivedDir );
            ControlFileMover.move( controlFile, targetControlFile );
            
            if ( r.isFailed() ) {
                // even though this operation was successful, we must not say success if the original item failed!
                return result.failed();
            } else {
                return Result.success( input, getOutput(controlFile, r.getOutput()) );
            }

        } catch ( final Throwable e ) {
            return result.failed( e );
        }
    }

    protected abstract Output getOutput( ControlFile controlFile, Input input );

}
