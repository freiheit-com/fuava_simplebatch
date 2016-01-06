package com.freiheit.fuava.simplebatch.processor;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.logging.JsonLogger;
import com.freiheit.fuava.simplebatch.result.Result;

public class JsonLoggingBatchedFailureProcessor<Input, Output> implements Processor<FetchedItem<Input>, Output, Output> {

    private final String dirName;
    private final String controlFileEnding;
    private final String logFileEnding;

    public JsonLoggingBatchedFailureProcessor( final String dirName, final String controlFileEnding, final String logFileEnding ) {
        this.dirName = dirName;
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
            final String failedPrefix = JsonLogger.nextFailedDownloadsName();
            final JsonLogger l = new JsonLogger( Paths.get( dirName, failedPrefix + logFileEnding ) );
            for ( final String failedInput : failedInputs ) {
                l.logWriteEnd( failedInput, false, failureMessages, failedIds.toString() );
            }
            ControlFileWriter.write(
                    Paths.get( dirName, failedPrefix + controlFileEnding ),
                    "DOWNLOAD_FAILED",
                    failedPrefix,
                    failedPrefix + logFileEnding );
        }
        return iterable;
    }

}
