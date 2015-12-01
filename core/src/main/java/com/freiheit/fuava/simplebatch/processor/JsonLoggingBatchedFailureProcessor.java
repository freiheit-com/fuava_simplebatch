package com.freiheit.fuava.simplebatch.processor;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.logging.JsonLogger;
import com.freiheit.fuava.simplebatch.result.Result;

public class JsonLoggingBatchedFailureProcessor<Input, Output> implements Processor<FetchedItem<Input>, Output, Output> {

    private String dirName;
    private String controlFileEnding;
    private String logFileEnding;

    public JsonLoggingBatchedFailureProcessor( String dirName, String controlFileEnding, String logFileEnding ) {
        this.dirName = dirName;
        this.controlFileEnding = controlFileEnding;
        this.logFileEnding = logFileEnding;
    }

    @Override
    public Iterable<Result<FetchedItem<Input>, Output>> process( Iterable<Result<FetchedItem<Input>, Output>> iterable ) {
        List<String> failedInputs = new ArrayList<String>();
        for ( Result<FetchedItem<Input>, Output> res : iterable ) {
            if ( res.isFailed() ) {
                Input input = res.getInput().getValue();
                String inputStr = input == null ? "null" : input.toString();
                failedInputs.add( inputStr );
            }
        }
        if ( !failedInputs.isEmpty() ) {
            String failedPrefix = JsonLogger.nextFailedDownloadsName();
            JsonLogger l = new JsonLogger( Paths.get( dirName, failedPrefix + logFileEnding ) );
            for ( String failedInput : failedInputs ) {
                l.logWriteEnd( failedInput, false );
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
