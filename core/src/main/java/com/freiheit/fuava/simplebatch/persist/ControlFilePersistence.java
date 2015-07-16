package com.freiheit.fuava.simplebatch.persist;

import java.io.File;
import java.io.FileWriter;
import java.io.OutputStreamWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Preconditions;


/**
 * @param <Input>
 * @param <Output>
 */
public class ControlFilePersistence<Input> extends SingleItemPersistence<Input, FilePersistenceOutputInfo, ControlFilePersistenceOutputInfo> {
    private static final Logger LOG = LoggerFactory.getLogger( ControlFilePersistence.class );
    
    public interface Configuration {
    	String getDownloadDirPath();
    	String getControlFileEnding();
    }

    
	private Configuration config;
	private File basedir;
    
    public ControlFilePersistence(Configuration config) {
		this.config = config;
		basedir = new File( Preconditions.checkNotNull(config.getDownloadDirPath()) );
	}
    

    @Override
    public Result<Input, ControlFilePersistenceOutputInfo> persistItem(Result<Input, FilePersistenceOutputInfo> r) {
		if (r.isFailed()) {
			return Result.<Input, ControlFilePersistenceOutputInfo>builder(r).failed();
		}
		Input input = r.getInput();
		try {		
			File f = r.getOutput().getDataFile();
			
			final File ctl = new File( basedir, String.valueOf( System.currentTimeMillis() ) + "_done" + config.getControlFileEnding() );
			LOG.info("Writing ctl file " + ctl  + " (exists: " + ctl.exists()  + ") " + trimOut(r.getOutput()));
			OutputStreamWriter fos2 = new FileWriter( ctl );
			try {
				fos2.write( f.getName() );
			} finally {
				fos2.flush();
				fos2.close();
			}
			if (!ctl.exists()) {
				return Result.failed(input, "Control file does not exist after write: " + ctl);
			}
			LOG.info("Wroite ctl file ");
			return Result.success(input, new ControlFilePersistenceOutputInfo(ctl));

		} catch ( final Throwable t ) {
			return Result.failed(input, t);
		}
	}
    
	private String trimOut(FilePersistenceOutputInfo output) {
		String os = output == null ? "null" : output.toString();
		return output == null? "null" : os.substring(0,  Math.min(20, os.length()));
	}

}
