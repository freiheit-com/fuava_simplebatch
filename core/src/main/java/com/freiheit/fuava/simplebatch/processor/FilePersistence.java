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

    private final FileOutputAdapter<Input, Output> adapter;
    private final File basedir;

    public FilePersistence( final String dir, final FileOutputAdapter<Input, Output> adapter ) {
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
