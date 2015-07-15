package com.freiheit.fuava.simplebatch.persist;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Charsets;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;


/**
 * @param <Input>
 * @param <Output>
 */
public class FilePersistence<Input, Output> implements Persistence<Input, Output> {

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
    
    private PersistenceAdapter<Input, Output> adapter;
    
    public FilePersistence(PersistenceAdapter<Input, Output> adapter) {
		this.adapter = adapter;
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
		String itemDescription = adapter.getItemDescription(r);
		
		File f = new File( basedir, itemDescription);
		try {
		    try ( OutputStreamWriter fos = new OutputStreamWriter( new FileOutputStream( f ), Charsets.UTF_8.name() ) ) {
		        if ( !r.isFailed() ) {
		        	adapter.write(fos, r.getOutput());
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
