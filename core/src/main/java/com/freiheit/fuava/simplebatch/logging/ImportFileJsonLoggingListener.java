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

    public ImportFileJsonLoggingListener( String downloadDir, String archivedDir, String failedDir ) {
        this.downloadDir = downloadDir;
        this.archivedDir = archivedDir;
        this.failedDir = failedDir;
    }

    @Override
    public void onFetchResult( Result<FetchedItem<ControlFile>, ControlFile> result ) {
        final String logFileName = result.getInput().getValue().getLogFileName();
        JsonLogger l = new JsonLogger( Paths.get( downloadDir, logFileName ) );
        l.logImportStart();
    }

    @Override
    public void onProcessingResult( Result<FetchedItem<ControlFile>, ResultStatistics> result ) {
        final String logFileName = result.getInput().getValue().getLogFileName();
        final String dir = result.isSuccess()
            ? archivedDir
            : failedDir;
        JsonLogger l = new JsonLogger( Paths.get( dir, logFileName ) );
        l.logImportEnd( result.isSuccess() );
    }
}