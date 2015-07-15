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
import java.io.FileInputStream;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

/**
 * .
 * 
 * @author tim.lessner@freiheit.com
 */

public class FileToInputStreamFunction<T> implements Function<File, T> {
    private static final Logger LOG = LoggerFactory.getLogger( FileToInputStreamFunction.class );
    private final Function<InputStream, T> func;

    public FileToInputStreamFunction(
    		Function<InputStream, T> func
	) {
        this.func = func;
    }

    @Override
    public T apply( final File file ) {
        try {
            final FileInputStream fis = new FileInputStream( file );
            return func.apply( fis );
        } catch ( final Exception e ) {
            LOG.error( e.getMessage(), e );
            throw new RuntimeException(e);
        }
    }
}
