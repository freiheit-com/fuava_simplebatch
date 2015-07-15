package com.freiheit.fuava.simplebatch;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

import com.freiheit.fuava.simplebatch.persist.Persistence;
import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Charsets;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;


/**
 * .
 * @param <Input>
 * @param <Output>
 */
public abstract class FilePersistence<Input, Output> implements Persistence<Input, Output> {

    public abstract String contentAsString( Input id, Output content );

    public abstract File newFile( File dir, Input id );

    static class FileWriterOutputInfo {
    	private final File ctrlFile;
    	public FileWriterOutputInfo(File ctrlFile) {
    		this.ctrlFile = ctrlFile;
		}
    	
    	@Override
    	public String toString() {
    		return MoreObjects.toStringHelper(this).add("Control File", ctrlFile).toString();
    	}
    }
    
    @Override
    public Iterable<Result<Input, FileWriterOutputInfo>> persist( final Iterable<Result<Input, Output>> iterable) {
    	ImmutableList.Builder<Result<Input, FileWriterOutputInfo>> b = ImmutableList.builder();
    	
        // FIXME: write mutliple entries in one file ?
        final File basedir = new File( ( "/tmp/downloading" ) );
        for ( Result<Input, Output> r : iterable ) {
            b.add(writeResult(basedir, r));
        }
        return b.build();
    }

	private Result<Input, FileWriterOutputInfo> writeResult(final File basedir, Result<Input, Output> r) {
		Input input = r.getInput();
		File f = newFile( basedir, input );
		try {
		    try ( OutputStreamWriter fos = new OutputStreamWriter( new FileOutputStream( f ), Charsets.UTF_8.name() ) ) {
		        if ( !r.isFailed() ) {
		            fos.write( contentAsString( input, r.getOutput() ) );
		        }
		        final File ctl = new File( basedir + "/" + String.valueOf( System.currentTimeMillis() ) + "_done.ctl" );
		        try ( OutputStreamWriter fos2 =
		                new OutputStreamWriter( new FileOutputStream( ctl ), Charsets.UTF_8.name() ) ) {
		            fos2.write( f.getName() );
		        }
		        return Result.success(input, new FileWriterOutputInfo(ctl));
		    }

		} catch ( final Throwable t ) {
			return Result.failed(input, t);
		}
	}
}
