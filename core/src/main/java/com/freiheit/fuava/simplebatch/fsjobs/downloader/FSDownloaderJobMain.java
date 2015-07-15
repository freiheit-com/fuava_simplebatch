package com.freiheit.fuava.simplebatch.fsjobs.downloader;

import com.freiheit.fuava.simplebatch.BatchJobMain;


/**
 * Helper class for implementing simple command line programs which import
 * data from files on the file system, implementing a control-files based protocol.
 * 
 * @author klas
 *
 */
public class FSDownloaderJobMain {

	public static <Input, Output> void exec(FSDownloaderJob<Input, Output> batchJob) {
		BatchJobMain.exec(batchJob);
	}

}
