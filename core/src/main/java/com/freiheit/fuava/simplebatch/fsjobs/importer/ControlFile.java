/*
 * (c) Copyright 2015 freiheit.com technologies GmbH
 *
 * Created on 14.07.15 by tim.lessner@freiheit.com
 *
 * This file contains unpublished, proprietary trade secret information of
 * freiheit.com technologies GmbH. Use, transcription, duplication and
 * modification are strictly prohibited without prior written consent of
 * freiheit.com technologies GmbH.
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
