package com.freiheit.fuava.simplebatch.fsjobs.importer;

import java.io.File;

import com.freiheit.fuava.simplebatch.processor.AbstractSingleItemProcessor;
import com.freiheit.fuava.simplebatch.result.Result;

public class FileMovingPersistence<D> extends AbstractSingleItemProcessor<ControlFile, D, D> {

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
    public Result<ControlFile, D> processItem(Result<ControlFile, D> r) {
        final ControlFile input = r.getInput();

        try {
            if (r.isFailed()) {
                moveBoth(input, failedDir);
            } else {
                moveBoth(input, archivedDir);
            }
            return Result.success(input, r.getOutput());
        } catch ( Throwable e ) {
            return Result.<ControlFile, D>builder(r).failed(e);
        }

    }

    private void moveBoth(final ControlFile input, String targetDirName)  throws FailedToMoveFileException {
        final File dir = new File(processingDir);
        final File targetDir = new File(targetDirName);
        if (!targetDir.exists()) {
            if (!targetDir.mkdirs()) {
                throw new FailedToMoveFileException("could not create directory " + processingDir);
            }
        }
        fileMover.moveFile( new File( dir, input.getFileName()), targetDirName );
        fileMover.moveFile( new File( dir, input.getControlledFileName() ), targetDirName );
    }

}