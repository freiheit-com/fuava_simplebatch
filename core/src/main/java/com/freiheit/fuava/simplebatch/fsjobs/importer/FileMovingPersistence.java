package com.freiheit.fuava.simplebatch.fsjobs.importer;

import java.io.File;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.processor.AbstractSingleItemProcessor;
import com.freiheit.fuava.simplebatch.result.Result;

public class FileMovingPersistence<D> extends AbstractSingleItemProcessor<FetchedItem<ControlFile>, D, D> {

    private final FileMover fileMover = new FileMover();
    private final String processingDir;
    private final String archivedDir;
    private final String failedDir;

    public FileMovingPersistence( final String processingDir, final String archivedDir, final String failedDir ) {
        super();
        this.processingDir = processingDir;
        this.archivedDir = archivedDir;
        this.failedDir = failedDir;
    }

    @Override
    public Result<FetchedItem<ControlFile>, D> processItem( final Result<FetchedItem<ControlFile>, D> r ) {
        final FetchedItem<ControlFile> input = r.getInput();
        final ControlFile controlFile = input.getValue();
        try {
            if ( r.isFailed() ) {
                moveBoth( controlFile, failedDir );
            } else {
                moveBoth( controlFile, archivedDir );
            }
            return Result.success( input, r.getOutput() );
        } catch ( final Throwable e ) {
            return Result.<FetchedItem<ControlFile>, D> builder( r ).failed( e );
        }

    }

    private void moveBoth( final ControlFile input, final String targetDirName ) throws FailedToMoveFileException {
        final File dir = new File( processingDir );
        final File targetDir = new File( targetDirName );
        if ( !targetDir.exists() ) {
            if ( !targetDir.mkdirs() ) {
                throw new FailedToMoveFileException( "could not create directory " + processingDir );
            }
        }
        fileMover.moveFile( new File( dir, input.getFileName() ), targetDirName );
        fileMover.moveFile( new File( dir, input.getControlledFileName() ), targetDirName );
    }

}