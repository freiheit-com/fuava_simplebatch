package com.freiheit.fuava.simplebatch.processor;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.logging.JsonLogger;
import com.freiheit.fuava.simplebatch.result.Result;

public class JsonLoggingBatchedFailureProcessor<Input, Output> implements Processor<FetchedItem<Input>, Output, Output> {

    private final Path baseDir;
    private final String controlFileEnding;
    private final String logFileEnding;

    public JsonLoggingBatchedFailureProcessor( final Path baseDir, final String controlFileEnding, final String logFileEnding ) {
        this.baseDir = baseDir;
        this.controlFileEnding = controlFileEnding;
        this.logFileEnding = logFileEnding;
    }

    @Override
    public Iterable<Result<FetchedItem<Input>, Output>> process( final Iterable<Result<FetchedItem<Input>, Output>> iterable ) {
        final List<String> failedInputs = new ArrayList<String>();
        final List<String> failedIds = new ArrayList<String>();
        final List<String> failureMessages = new ArrayList<String>();
        for ( final Result<FetchedItem<Input>, Output> res : iterable ) {
            if ( res.isFailed() ) {
                final FetchedItem<Input> fetchedItem = res.getInput();
                final Input input = fetchedItem == null ? null : fetchedItem.getValue();
                final String inputStr = input == null ? "null" : input.toString();
                failedInputs.add( inputStr );
                final String idString = fetchedItem == null
                    ? null
                    : fetchedItem.getIdentifier();
                if ( idString != null ) {
                    failedIds.add( idString );
                }
                failureMessages.addAll( res.getAllMessages() );
            }
        }
        if ( !failedInputs.isEmpty() ) {
            final Path failedPrefix = JsonLogger.nextFailedDownloadsName();
            final Path dataFile = baseDir.resolve( failedPrefix );
            final Path logFilePath = FileUtil.getLogFilePath( dataFile, logFileEnding );
            final JsonLogger l = new JsonLogger( logFilePath );
            for ( final String failedInput : failedInputs ) {
                l.logWriteEnd( failedInput, false, failureMessages, failedIds.toString() );
            }
            final Path controlFilePath = FileUtil.getControlFilePath( dataFile, controlFileEnding );
            ControlFileWriter.write(
                    controlFilePath,
                    "DOWNLOAD_FAILED",
                    baseDir,
                    dataFile,
                    logFilePath );
        }
        return iterable;
    }

}
