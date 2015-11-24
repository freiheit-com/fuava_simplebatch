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
    private final File file;

    public ControlFile( final String sourceDir, final String pathToControlledFile, final File file ) {
        this.controlledFile = new File( sourceDir, pathToControlledFile );
        this.file = file;
    }

    public File getControlledFile() {
        return controlledFile;
    }

    public String getControlledFileName() {
        return controlledFile.getName();
    }

    public String getFileName() {
        return file.getName();
    }

    public File getFile() {
        return file;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper( this )
                .add( "file", controlledFile )
                .add( "ctl", file )
                .toString();
    }
}
