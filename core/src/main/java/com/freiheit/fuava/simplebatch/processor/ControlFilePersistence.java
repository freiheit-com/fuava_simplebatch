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
import java.io.OutputStreamWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

/**
 * @param <Input>
 * @param <Output>
 */
class ControlFilePersistence<Input> extends
        AbstractSingleItemProcessor<Input, FilePersistenceOutputInfo, ControlFilePersistenceOutputInfo> {
    private static final Logger LOG = LoggerFactory.getLogger( ControlFilePersistence.class );

    public interface Configuration {
        String getDownloadDirPath();

        String getControlFileEnding();
    }

    private final File basedir;

    public ControlFilePersistence( final Configuration config ) {
        basedir = new File( Preconditions.checkNotNull( config.getDownloadDirPath() ) );
    }

    @Override
    public Result<Input, ControlFilePersistenceOutputInfo> processItem( final Result<Input, FilePersistenceOutputInfo> r ) {
        if ( r.isFailed() ) {
            return Result.<Input, ControlFilePersistenceOutputInfo> builder( r ).failed();
        }
        final Input input = r.getInput();
        try {
            final File f = r.getOutput().getDataFile();

            final File ctl = new File( basedir, f.getName() + ".ctl"/*
                                                                     * nextControlFilename
                                                                     * ()
                                                                     */);
            LOG.info( "Writing ctl file " + ctl + " (exists: " + ctl.exists() + ") " + trimOut( r.getOutput() ) );
            final OutputStreamWriter fos2 = new OutputStreamWriter( new FileOutputStream( ctl ), Charsets.UTF_8 );
            try {
                fos2.write( f.getName() );
            } finally {
                fos2.flush();
                fos2.close();
            }
            if ( !ctl.exists() ) {
                return Result.failed( input, "Control file does not exist after write: " + ctl );
            }
            LOG.info( "Wrote ctl file " );
            return Result.success( input, new ControlFilePersistenceOutputInfo( ctl ) );

        } catch ( final Throwable t ) {
            return Result.failed( input, t );
        }
    }

    private String trimOut( final FilePersistenceOutputInfo output ) {
        final String os = output == null
            ? "null"
            : output.toString();
        return output == null
            ? "null"
            : os.substring( 0, Math.min( 20, os.length() ) );
    }

}
