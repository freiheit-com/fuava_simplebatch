package com.freiheit.fuava.simplebatch.fsjobs.importer;

import java.io.File;

import com.freiheit.fuava.simplebatch.persist.SingleItemPersistence;
import com.freiheit.fuava.simplebatch.result.Result;

public class FileMovingPersistence<D> extends SingleItemPersistence<ControlFile, D, Void> {

	private final FileMover fileMover = new FileMover();
	private final String processingDir;
	private final String archivedDir;
	private final String failedDir;

	public FileMovingPersistence(String processingDir, String archivedDir, String failedDir) {
		super();
		this.processingDir = processingDir;
		this.archivedDir = archivedDir;
		this.failedDir = failedDir;
	}

	@Override
	public Result<ControlFile, Void> persistItem(Result<ControlFile, D> r) {
		final ControlFile input = r.getInput();

		try {
			if (r.isFailed()) {
				moveBoth(input, failedDir);
			} else {
				moveBoth(input, archivedDir);
			}
			return Result.success(input, null);
		} catch ( Throwable e ) {
			return Result.<ControlFile, Void>builder(r).withThrowable(e).failed();
		}

	}

	private void moveBoth(final ControlFile input, String targetDir)  throws FailedToMoveFileException {
		final String pathname = processingDir + "/" + input.getPathToControlledFile();
		fileMover.moveFile( input.getFile(), targetDir );
		fileMover.moveFile( new File( pathname ), targetDir );
	}

}