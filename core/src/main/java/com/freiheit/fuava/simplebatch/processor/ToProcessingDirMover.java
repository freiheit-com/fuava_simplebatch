package com.freiheit.fuava.simplebatch.processor;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fsjobs.importer.ControlFile;
import com.freiheit.fuava.simplebatch.result.Result;

/**
 * Moves the control file and its associated files to the processing dir, prepending the file names with the given instanceid.
 * 
 * @author klas
 */
public final class ToProcessingDirMover extends AbstractSingleItemProcessor<FetchedItem<ControlFile>, ControlFile, File> {
    
    private static final String INSTANCE_ID_PREFIX_SEPARATOR = "-";
    private final Path processingDir;
    private final String instanceId;

    public ToProcessingDirMover( final Path processingDir, final String instanceId ) {
        this.processingDir = processingDir;
        this.instanceId = instanceId;
    }

    @Override
    public Result<FetchedItem<ControlFile>, File> processItem( final Result<FetchedItem<ControlFile>, ControlFile> r ) {
        final Result.Builder<FetchedItem<ControlFile>, File> result = Result.<FetchedItem<ControlFile>, File>builder( r );
        final FetchedItem<ControlFile> input = r.getInput();
        try {
            final ControlFile controlFile = input == null ? null : input.getValue();
            if ( controlFile == null ) {
                return result.withFailureMessage( "Cannot process null Control File" ).failed();
            }
            
            // We always need to move to the processing directory, because else the processing steps would fail 
            // when they try to process the result and move it to archive/failed directories
            final ControlFile targetControlFile = controlFile.withBaseDir( processingDir ).withFilePrefix( createInstanceIdPrefix( instanceId ) );
            ControlFileMover.move( controlFile, targetControlFile );

            if ( !Files.exists( targetControlFile.getControlFile() ) ) {
                return result.withFailureMessage( "ControlFile " + targetControlFile.getControlFile() + " did not exist after move!" ).failed();
            }
            
            // we need to update the contents of the control file to reflect the new name before we proceed 
            ControlFileWriter.write( targetControlFile );

            final Path controlledFile = targetControlFile.getControlledFile();
            if ( r.isFailed() ) {
                return result
                        // We need to create a new fetched item due to the new location of the control file
                        .withInput( input.withValue( targetControlFile ) )
                        .failed();
            } else if (controlledFile == null) {
                return result
                        // We need to create a new fetched item due to the new location of the control file
                        .withInput( input.withValue( targetControlFile ) )
                        .withFailureMessage( "No controlled file" )
                        .failed();
            } else {
                return result
                        // We need to create a new fetched item due to the new location of the control file
                        .withInput( input.withValue( targetControlFile ) )
                        .withOutput( controlledFile.toFile() )
                        .success();
            }

        } catch ( final Throwable e ) {
            return result.failed( e );
        }
    }

    public static String createInstanceIdPrefix( final String instanceId ) {
        return instanceId + INSTANCE_ID_PREFIX_SEPARATOR;
    }
}