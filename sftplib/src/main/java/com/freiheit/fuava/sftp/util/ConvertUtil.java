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
package com.freiheit.fuava.sftp.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Applies a function to a collection.
 *
 * @author tim.lessner@freiheit.com
 */

public class ConvertUtil {

    private static final Logger LOG = LoggerFactory.getLogger( ConvertUtil.class );
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 4;


    private ConvertUtil() {

    }

    /**
     * Applies a function to a collection.
     */
    @Nonnull
    public static <T, U> List<U> convertList( final List<T> from, final Function<T, U> func ) {
        return from.stream().map( func ).collect( Collectors.toList() );
    }

    /**
     * This function is taken from IOUtils but adjusted with integrated logging.
     *
     * @param input is input stream from sftp file
     * @param output stream to downloaded csv file
     */
    public static long copyLargeWithLoggingProgress( final InputStream input, final OutputStream output ) throws IOException {
        final byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
        long count = 0;
        long writtenBytesCounter = 0;
        int n = 0;
        while ( -1 != ( n = input.read( buffer ) ) ) {
            output.write( buffer, 0, n );
            count += n;
            writtenBytesCounter += DEFAULT_BUFFER_SIZE;
            if ( writtenBytesCounter % 25000000 < DEFAULT_BUFFER_SIZE ) {
                LOG.info( "Successfully downloaded (mB) : " + writtenBytesCounter / 1000000 );
            }
        }
        return count;
    }

}
