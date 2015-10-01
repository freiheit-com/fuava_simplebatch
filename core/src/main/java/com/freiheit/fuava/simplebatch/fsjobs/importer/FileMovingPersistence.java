package com.freiheit.fuava.simplebatch.fsjobs.importer;

import java.io.File;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.processor.AbstractSingleItemProcessor;
import com.freiheit.fuava.simplebatch.result.Result;

public class FileMovingPersistence<D> extends AbstractSingleItemProcessor<FetchedItem<ControlFile>, D, D> {

    private final FileMover fileMover = new FileMover();
    private final File processingDir;
    private final File archivedDir;
    private final File failedDir;

    public FileMovingPersistence( final String processingDir, final String archivedDir, final String failedDir ) {
        this( new File( processingDir ), new File( archivedDir ), new File( failedDir ) );
    }

    public FileMovingPersistence( final File processingDir, final File archivedDir, final File failedDir ) {
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
                // even though this operation was successful, we must not say success if the original item failed!
                return Result.<FetchedItem<ControlFile>, D> builder( r ).failed();

            } else {
                moveBoth( controlFile, archivedDir );
                return Result.success( input, r.getOutput() );
            }

        } catch ( final Throwable e ) {
            return Result.<FetchedItem<ControlFile>, D> builder( r ).failed( e );
        }

    }

    private void moveBoth( final ControlFile input, final File targetDir ) throws FailedToMoveFileException {
        final File dir = processingDir;
        if ( !targetDir.exists() ) {
            if ( !targetDir.mkdirs() ) {
                throw new FailedToMoveFileException( "could not create directory " + processingDir );
            }
        }
        fileMover.moveFile( new File( dir, input.getFileName() ), targetDir );
        fileMover.moveFile( new File( dir, input.getControlledFileName() ), targetDir );
    }

}