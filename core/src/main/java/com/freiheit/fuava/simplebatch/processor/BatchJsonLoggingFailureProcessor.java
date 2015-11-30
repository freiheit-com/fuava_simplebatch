package com.freiheit.fuava.simplebatch.processor;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.logging.BatchJsonLogger;
import com.freiheit.fuava.simplebatch.result.Result;

public class BatchJsonLoggingFailureProcessor<Input, Output> implements Processor<FetchedItem<Input>, Output, Output> {

    private String dirName;
    private String controlFileEnding;
    private String logFileEnding;

    public BatchJsonLoggingFailureProcessor( String dirName, String controlFileEnding, String logFileEnding ) {
        this.dirName = dirName;
        this.controlFileEnding = controlFileEnding;
        this.logFileEnding = logFileEnding;
    }

    @Override
    public Iterable<Result<FetchedItem<Input>, Output>> process( Iterable<Result<FetchedItem<Input>, Output>> iterable ) {
        List<String> failedInputs = new ArrayList<String>();
        for ( Result<FetchedItem<Input>, Output> res : iterable ) {
            if ( res.isFailed() ) {
                failedInputs.add( res.getInput().getValue().toString() );
            }
        }
        if ( !failedInputs.isEmpty() ) {
            String failedPrefix = BatchJsonLogger.nextFailedDownloadsName();
            BatchJsonLogger l = new BatchJsonLogger( Paths.get( dirName, failedPrefix + logFileEnding ) );
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
