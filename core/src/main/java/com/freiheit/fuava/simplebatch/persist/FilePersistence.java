package com.freiheit.fuava.simplebatch.persist;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;


/**
 * @param <Input>
 * @param <Output>
 */
public class FilePersistence<Input, Output> implements Persistence<Input, Output, FilePersistenceOutputInfo> {

    public interface Configuration {
    	String getDownloadDirPath();
    }


    private PersistenceAdapter<Input, Output> adapter;
	private Configuration configuration;
    
    public FilePersistence(Configuration configuration, PersistenceAdapter<Input, Output> adapter) {
		this.configuration = Preconditions.checkNotNull(configuration);
		this.adapter = Preconditions.checkNotNull(adapter);
	}
    
    @Override
    public Iterable<Result<Input, FilePersistenceOutputInfo>> persist( final Iterable<Result<Input, Output>> iterable) {
    	ImmutableList.Builder<Result<Input, FilePersistenceOutputInfo>> b = ImmutableList.builder();
    	
        // FIXME: write mutliple entries in one file ?
        final File basedir = new File( configuration.getDownloadDirPath() );
        for ( Result<Input, Output> r : iterable ) {
            b.add(writeResult(basedir, r));
        }
        return b.build();
    }

	private Result<Input, FilePersistenceOutputInfo> writeResult(final File basedir, Result<Input, Output> r) {
		Input input = r.getInput();
		String itemDescription = adapter.getItemDescription(r);
		
		File f = new File( basedir, itemDescription);
		try {
		    try ( OutputStreamWriter fos = new OutputStreamWriter( new FileOutputStream( f ), Charsets.UTF_8.name() ) ) {
		        if ( !r.isFailed() ) {
		        	adapter.write(fos, r.getOutput());
		        }
		        return Result.success(input, new FilePersistenceOutputInfo(f));
		    }

		} catch ( final Throwable t ) {
			return Result.failed(input, "Failed writing to file " + f.getAbsolutePath(), t);
		}
	}
}
