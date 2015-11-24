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
package com.freiheit.fuava.simplebatch.processor;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Preconditions;

/**
 * @param <Input>
 * @param <Output>
 */
class FilePersistence<Input, Output> extends AbstractSingleItemProcessor<Input, Output, FilePersistenceOutputInfo> {
    private static final Logger LOG = LoggerFactory.getLogger( FilePersistence.class );

    private final FileOutputStreamAdapter<Input, Output> adapter;
    private final File basedir;

    public FilePersistence( final String dir, final FileOutputStreamAdapter<Input, Output> adapter ) {
        this.adapter = Preconditions.checkNotNull( adapter );
        this.basedir = new File( Preconditions.checkNotNull( dir ) );
        if ( !this.basedir.exists() ) {
            if ( this.basedir.mkdirs() ) {
                LOG.info( "Created base dir ", basedir );
            } else {
                LOG.error( "Could not create base dir ", basedir );
            }
        }
    }

    @Override
    public Result<Input, FilePersistenceOutputInfo> processItem( final Result<Input, Output> r ) {
        if ( r.isFailed() ) {
            return Result.<Input, FilePersistenceOutputInfo> builder( r ).failed();
        }

        final Input input = r.getInput();
        File f = null;
        try {
            final String itemDescription = adapter.getFileName( r );
            f = new File( basedir, itemDescription );
            LOG.info( "Writing data file " + f + " (exists: " + f.exists() + ") " + trimOut( r.getOutput() ) );
            try ( OutputStream fos = new FileOutputStream( f ) ) {
                adapter.writeToStream( fos, r.getOutput() );
                fos.flush();
            }
            if ( !f.exists() ) {
                return Result.failed( input, "Control file does not exist after write: " + f );
            }
            LOG.info( "Wrote data file " + f );
            return Result.success( input, new FilePersistenceOutputInfo( f ) );

        } catch ( final Throwable t ) {
            return Result.failed( input, "Failed writing to file " + ( f == null
                ? null
                : f.getAbsolutePath() ), t );
        }
    }

    private String trimOut( final Output output ) {
        final String os = output == null
            ? "null"
            : output.toString();
        return output == null
            ? "null"
            : os.substring( 0, Math.min( 20, os.length() ) );
    }
}
