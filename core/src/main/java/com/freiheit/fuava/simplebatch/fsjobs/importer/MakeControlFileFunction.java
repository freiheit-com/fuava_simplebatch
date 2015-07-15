/*
 * (c) Copyright 2015 freiheit.com technologies GmbH
 *
 * Created on 15.07.15 by tim.lessner@freiheit.com
 *
 * This file contains unpublished, proprietary trade secret information of
 * freiheit.com technologies GmbH. Use, transcription, duplication and
 * modification are strictly prohibited without prior written consent of
 * freiheit.com technologies GmbH.
 */

package com.freiheit.fuava.simplebatch.fsjobs.importer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.google.common.base.Function;

/**
 * @author tim.lessner@freiheit.com
 */
public class MakeControlFileFunction implements Function<File, ControlFile> {
    @Override
    public ControlFile apply( final File file ) {
        try {
            final FileReader in = new FileReader( file );
            final BufferedReader br = new BufferedReader( in );
            final String nameOfDownloadedMiscDocument = br.readLine();
            return new ControlFile( nameOfDownloadedMiscDocument, file );
        } catch ( final IOException e ) {
            throw new RuntimeException( e );
        }
    }
}
