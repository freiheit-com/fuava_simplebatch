package com.freiheit.fuava.simplebatch.logging;

import java.nio.file.Paths;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fsjobs.importer.ControlFile;
import com.freiheit.fuava.simplebatch.result.ProcessingResultListener;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;

public class ImportFileJsonLoggingListener implements ProcessingResultListener<ControlFile, ResultStatistics> {

    private final String downloadDir;
    private final String archivedDir;
    private final String failedDir;

    public ImportFileJsonLoggingListener( final String downloadDir, final String archivedDir, final String failedDir ) {
        this.downloadDir = downloadDir;
        this.archivedDir = archivedDir;
        this.failedDir = failedDir;
    }

    @Override
    public void onFetchResult( final Result<FetchedItem<ControlFile>, ControlFile> result ) {
        final String logFileName = getLogfileName( result );
        final FetchedItem<ControlFile> fetchedItem = result.getInput();
        final String identifier = fetchedItem == null
            ? null
            : fetchedItem.getIdentifier();

        final JsonLogger l = new JsonLogger( Paths.get( downloadDir, logFileName ) );
        l.logImportStart( identifier );
    }

    private String getLogfileName( final Result<FetchedItem<ControlFile>, ?> result ) {
        final FetchedItem<ControlFile> fetchedItem = result.getInput();
        final ControlFile value = fetchedItem == null ? null : fetchedItem.getValue();
        final String logFileName = value == null ? "failed_control_files.log": value.getLogFileName();
        return logFileName;
    }

    @Override
    public void onProcessingResult( final Result<FetchedItem<ControlFile>, ResultStatistics> result ) {
        final FetchedItem<ControlFile> fetchedItem = result.getInput();
        final String identifier = fetchedItem == null
            ? null
            : fetchedItem.getIdentifier();

        final String logFileName = getLogfileName( result );
        final String dir = result.isSuccess()
            ? archivedDir
            : failedDir;
        final JsonLogger l = new JsonLogger( Paths.get( dir, logFileName ) );
        l.logImportEnd( result.isSuccess(), result.getAllMessages(), identifier );
    }
}