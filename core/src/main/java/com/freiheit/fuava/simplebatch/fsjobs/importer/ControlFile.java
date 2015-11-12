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

import java.io.File;

import com.google.common.base.MoreObjects;

/**
 * @author tim.lessner@freiheit.com
 */
public class ControlFile {

    private final File controlledFile;
    private final File logFile;
    private final File file;
    private final boolean downloadFailed;

    public ControlFile( final String sourceDir, final String pathToControlledFile, final File file ) {
    	this(sourceDir, pathToControlledFile, file, false);        
    }        
    
    public ControlFile( final String sourceDir, final String pathToControlledFile, final File file, boolean downloadFailed) {
    	this.controlledFile = downloadFailed ? null : new File( sourceDir, pathToControlledFile );
    	this.logFile = new File( sourceDir, pathToControlledFile + ".log");
    	this.file = file;
    	this.downloadFailed = downloadFailed;    			
    }
    
    public File getControlledFile() {
        return controlledFile;
    }

    public String getControlledFileName() {
        return controlledFile == null ? null : controlledFile.getName();
    }

    public String getLogFileName() {
        return logFile.getName();
    }

    public File getLogFile() {
        return logFile;
    }    
    
    public String getFileName() {
        return file.getName();
    }

    public File getFile() {
        return file;
    }
    
    public boolean hasDownloadFailed() {
    	return downloadFailed;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper( this )
                .add( "file", controlledFile )
                .add( "ctl", file )
                .add( "log", logFile)
                .toString();
    }
}
