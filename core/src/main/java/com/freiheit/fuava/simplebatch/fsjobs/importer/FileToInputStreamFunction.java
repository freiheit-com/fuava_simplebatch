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
package com.freiheit.fuava.simplebatch.fsjobs.importer;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
