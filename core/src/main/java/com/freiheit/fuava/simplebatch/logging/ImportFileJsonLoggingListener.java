package com.freiheit.fuava.simplebatch.logging;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fsjobs.importer.ControlFile;
import com.freiheit.fuava.simplebatch.result.ProcessingResultListener;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;

public class ImportFileJsonLoggingListener implements ProcessingResultListener<ControlFile, ResultStatistics> {
    private final Path downloadDir;
    private final Path archivedDir;
    private final Path failedDir;

    public ImportFileJsonLoggingListener( final Path downloadDir, final Path archivedDir, final Path failedDir ) {
        this.downloadDir = downloadDir;
        this.archivedDir = archivedDir;
        this.failedDir = failedDir;
    }

    @Override
    public void onFetchResult( final Result<FetchedItem<ControlFile>, ControlFile> result ) {
        final Path logFileRelPath = getLogfileRelPath( result );
        final FetchedItem<ControlFile> fetchedItem = result.getInput();
        final String identifier = fetchedItem == null
            ? null
            : fetchedItem.getIdentifier();

        final JsonLogger l = new JsonLogger( downloadDir.resolve( logFileRelPath ) );
        l.logImportStart( identifier );
    }

    private Path getLogfileRelPath( final Result<FetchedItem<ControlFile>, ?> result ) {
        final FetchedItem<ControlFile> fetchedItem = result.getInput();
        final ControlFile value = fetchedItem == null ? null : fetchedItem.getValue();
        return value == null ? Paths.get( "failed_control_files.log" ): value.getLogFileRelPath();
    }

    @Override
    public void onProcessingResult( final Result<FetchedItem<ControlFile>, ResultStatistics> result ) {
        final FetchedItem<ControlFile> fetchedItem = result.getInput();
        final String identifier = fetchedItem == null
            ? null
            : fetchedItem.getIdentifier();

        final Path logFileName = getLogfileRelPath( result );
        final Path dir = result.isSuccess()
            ? archivedDir
            : failedDir;
        final JsonLogger l = new JsonLogger( dir.resolve( logFileName ) );
        l.logImportEnd( result.isSuccess(), result.getAllMessages(), identifier );
    }
}