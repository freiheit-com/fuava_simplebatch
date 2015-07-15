package com.freiheit.fuava.simplebatch.persist;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;


/**
 * @param <Input>
 * @param <Output>
 */
public class ControlFilePersistence<Input> implements Persistence<Input, FilePersistenceOutputInfo, ControlFilePersistenceOutputInfo> {
	
    public interface Configuration {
    	String getDownloadDirPath();
    	String getControlFileEnding();
    }

    
	private Configuration config;
    
    public ControlFilePersistence(Configuration config) {
		this.config = config;
	}
    
    @Override
    public Iterable<Result<Input, ControlFilePersistenceOutputInfo>> persist( final Iterable<Result<Input, FilePersistenceOutputInfo>> iterable) {
    	ImmutableList.Builder<Result<Input, ControlFilePersistenceOutputInfo>> b = ImmutableList.builder();

        final File basedir = new File( config.getDownloadDirPath() );
        for ( Result<Input, FilePersistenceOutputInfo> r : iterable ) {
            b.add(writeResult(basedir, r));
        }
        return b.build();
    }

	private Result<Input, ControlFilePersistenceOutputInfo> writeResult(final File basedir, Result<Input, FilePersistenceOutputInfo> r) {
		Input input = r.getInput();
		try {		
			File f = r.getOutput().getDataFile();
			final File ctl = new File( basedir + "/" + String.valueOf( System.currentTimeMillis() ) + "_done" + config.getControlFileEnding() );
			try ( OutputStreamWriter fos2 =
					new OutputStreamWriter( new FileOutputStream( ctl ), Charsets.UTF_8.name() ) ) {
				fos2.write( f.getName() );
			}
			return Result.success(input, new ControlFilePersistenceOutputInfo(ctl));

		} catch ( final Throwable t ) {
			return Result.failed(input, t);
		}
	}
}
