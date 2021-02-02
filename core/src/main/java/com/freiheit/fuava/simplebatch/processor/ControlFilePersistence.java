/*
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

import com.freiheit.fuava.simplebatch.result.Result;

import java.io.File;
import java.nio.file.Path;
import java.util.Objects;

/**
 * @param <Input>
 */
class ControlFilePersistence<Input> extends
        AbstractSingleItemProcessor<Input, FilePersistenceOutputInfo, ControlFilePersistenceOutputInfo> {

    public interface Configuration {
        Path getDownloadDirPath();

        String getControlFileEnding();

        String getLogFileEnding();
    }

    private final Configuration config;
    private final Path downloadDirPath;
    
    public ControlFilePersistence( final Configuration config ) {
        this.config = config;
        this.downloadDirPath = Objects.requireNonNull( config.getDownloadDirPath() );
    }

    @Override
    public Result<Input, ControlFilePersistenceOutputInfo> processItem( final Result<Input, FilePersistenceOutputInfo> r ) {
        if ( r.isFailed() ) {
            return Result.<Input, ControlFilePersistenceOutputInfo> builder( r ).failed();
        }
        final Input input = r.getInput();
        try {
            final Path dataFile = r.getOutput().getDataFile();

            final Path ctlFile = FileUtil.getControlFilePath( dataFile, config.getControlFileEnding() );
            final Path logFile = FileUtil.getLogFilePath( dataFile, config.getLogFileEnding() );
            
            ControlFileWriter.write( ctlFile, "DOWNLOAD_SUCCESSFUL", downloadDirPath, dataFile , logFile );

            final File ctl = ctlFile.toFile();
            if ( !ctl.exists() ) {
                return Result.failed( input, "Control file does not exist after write: " + ctl );
            }
            return Result.success( input, new ControlFilePersistenceOutputInfo( ctlFile ) );

        } catch ( final Throwable t ) {
            return Result.failed( input, t );
        }
    }
}
