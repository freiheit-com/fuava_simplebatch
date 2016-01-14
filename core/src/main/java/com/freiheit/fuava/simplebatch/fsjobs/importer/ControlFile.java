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
package com.freiheit.fuava.simplebatch.fsjobs.importer;

import java.nio.file.Path;

import javax.annotation.CheckForNull;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

/**
 * @author tim.lessner@freiheit.com
 */
public class ControlFile {

    private final Path relControlledFile;
    private final Path relLogFile;
    private final Path relControlFile;
    private final boolean downloadFailed;

    public ControlFile( final Path downloadsDir, final Path controlledFileRelPath, final Path logFileRelPath, final Path controlFile ) {
        this( downloadsDir, controlledFileRelPath, logFileRelPath, controlFile, false );
    }

    public ControlFile( final Path downloadsDir, final Path controlledFileRelPath, final Path logFileRelPath, final Path controlFile,
            final boolean downloadFailed ) {
        Preconditions.checkArgument( !controlledFileRelPath.isAbsolute(), "Controlled File Path must not be absolute" );
        Preconditions.checkArgument( !logFileRelPath.isAbsolute(), "Log File Path must not be absolute" );
        Preconditions.checkArgument( downloadsDir.isAbsolute(), "Base Dir Path must be absolute" );
        Preconditions.checkArgument( controlFile.isAbsolute(), "Control File Path must be absolute" );
        Preconditions.checkArgument( controlFile.startsWith( downloadsDir ), "Control File Path must begin with the base dir" );
        
        this.relControlledFile = downloadFailed ? null : controlledFileRelPath;
        this.relLogFile = logFileRelPath;
        this.relControlFile = downloadsDir.relativize( controlFile );
        this.downloadFailed = downloadFailed;
    }

    
    public Path getControlFileRelPath() {
        return relControlFile;
    }
    
    @CheckForNull
    public Path getControlledFileRelPath() {
        return relControlledFile;
    }
    
    public Path getLogFileRelPath() {
        return relLogFile;
    }
    

    public boolean hasDownloadFailed() {
        return downloadFailed;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper( this ).add( "file", this.relControlledFile ).add( "ctl", this.relControlFile ).add( "log", this.relLogFile ).toString();
    }

}
