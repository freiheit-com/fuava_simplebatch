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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import com.google.common.base.Charsets;
import com.google.common.base.Function;

/**
 * @author tim.lessner@freiheit.com
 */
public class ReadControlFileFunction implements Function<File, ControlFile> {

    private final String baseDir;

    public ReadControlFileFunction( final String baseDir ) {
        this.baseDir = baseDir;
    }

    @Override
    public ControlFile apply( final File file ) {
        try {
            final Reader in = new InputStreamReader( new FileInputStream( file ), Charsets.UTF_8 );
            try ( BufferedReader br = new BufferedReader( in ) ) {
                final String nameOfDownloadedMiscDocument = br.readLine();
                return new ControlFile( this.baseDir, nameOfDownloadedMiscDocument, file );
            }
        } catch ( final IOException e ) {
            throw new RuntimeException( e );
        }
    }
}
