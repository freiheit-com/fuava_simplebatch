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
            final Function<InputStream, T> func ) {
        this.func = func;
    }

    @Override
    public T apply( final File file ) {
        try {
            final FileInputStream fis = new FileInputStream( file );
            return func.apply( fis );
        } catch ( final Exception e ) {
            LOG.error( e.getMessage(), e );
            throw new RuntimeException( e );
        }
    }
}
