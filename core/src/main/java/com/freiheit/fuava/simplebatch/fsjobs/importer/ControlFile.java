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
import javax.annotation.Nullable;

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
    private final Path baseDir;
    private final String status;
    private final String originalControlledFileName;

    public ControlFile( final Path downloadsDir, final Path controlledFileRelPath, final Path logFileRelPath, final Path controlFileRelPath ) {
        this( downloadsDir, controlledFileRelPath, logFileRelPath, controlFileRelPath, controlledFileRelPath.toString(), null );
    }

    public ControlFile( 
            final Path baseDir, 
            final Path controlledFileRelPath, 
            final Path logFileRelPath, 
            final Path controlFileRelPath,
            final String originalControlledFileName,
            @Nullable final String status 
    ) {
        this.originalControlledFileName = originalControlledFileName;
        Preconditions.checkArgument( !controlledFileRelPath.isAbsolute(), "Controlled File Path must not be absolute" );
        Preconditions.checkArgument( !logFileRelPath.isAbsolute(), "Log File Path must not be absolute" );
        Preconditions.checkArgument( !controlFileRelPath.isAbsolute(), "Control File Path must not be absolute" );
        Preconditions.checkArgument( baseDir.isAbsolute(), "Base Dir Path must be absolute" );
        
        this.status = status;
        this.downloadFailed = "DOWNLOAD_FAILED".equals( status );;
        this.baseDir = baseDir;
        this.relControlledFile = downloadFailed ? null : controlledFileRelPath;
        this.relLogFile = logFileRelPath;
        this.relControlFile = controlFileRelPath;
    }

    public ControlFile withBaseDir( final Path baseDir ) {
        return new ControlFile( baseDir, relControlledFile, relLogFile, relControlFile, this.originalControlledFileName, status );
    }

    public ControlFile withFilePrefix( final String prefix ) {
        return new ControlFile( baseDir, prefix( prefix, relControlledFile ), prefix( prefix, relLogFile ), prefix( prefix, relControlFile ), this.originalControlledFileName, status );
    }

    private Path prefix( final String prefix, final Path relPath ) {
        if ( relPath == null ) {
            return relPath;
        }
        final String newName = prefix + relPath.getFileName().toString();
        return relPath.resolveSibling( newName );
    }

    public String getOriginalControlledFileName() {
        return originalControlledFileName;
    }
    
    public Path getControlFileRelPath() {
        return relControlFile;
    }
    public Path getControlFile() {
        return getFullPath( relControlFile );
    }
    
    public String getStatus() {
        return status;
    }
    
    
    @CheckForNull
    public Path getControlledFileRelPath() {
        return relControlledFile;
    }

    @CheckForNull
    public Path getControlledFile() {
        return getFullPath( relControlledFile );
    }
    
    public Path getLogFileRelPath() {
        return relLogFile;
    }
    public Path getLogFile() {
        return getFullPath( relLogFile );
    }
    
    private Path getFullPath( final Path relPath ) { 
        return relPath == null ? null : baseDir.resolve( relPath );
    }

    public boolean hasDownloadFailed() {
        return downloadFailed;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper( this ).add( "file", this.relControlledFile ).add( "ctl", this.relControlFile ).add( "log", this.relLogFile ).toString();
    }

}
