package com.freiheit.fuava.simplebatch.processor;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.logging.JsonLogger;
import com.freiheit.fuava.simplebatch.result.Result;

public class JsonLoggingProcessor<Input>
        implements Processor<FetchedItem<Input>, FilePersistenceOutputInfo, FilePersistenceOutputInfo> {

    private String dirName;
    private String controlFileEnding;
    private String logFileEnding;

    public JsonLoggingProcessor( String dirName, String controlFileEnding, String logFileEnding ) {
        this.dirName = dirName;
        this.controlFileEnding = controlFileEnding;
        this.logFileEnding = logFileEnding;
    }

    @Override
    public Iterable<Result<FetchedItem<Input>, FilePersistenceOutputInfo>> process(
            Iterable<Result<FetchedItem<Input>, FilePersistenceOutputInfo>> iterable ) {
        for ( Result<FetchedItem<Input>, FilePersistenceOutputInfo> res : iterable ) {
            Path logFile;
            String failedPrefix = "";
            if ( res.isFailed() ) {
                failedPrefix = JsonLogger.nextFailedDownloadsName();
                logFile = Paths.get( dirName, failedPrefix + logFileEnding );
            } else {
                logFile = Paths.get( res.getOutput().getDataFile() + logFileEnding );
            }
            JsonLogger l = new JsonLogger( logFile );
            Input input = res.getInput().getValue();
            String inputStr = input == null
                ? "null"
                : input.toString();
            l.logWriteEnd( inputStr, res.isSuccess() );
            if ( res.isFailed() ) {
                ControlFileWriter.write(
                        Paths.get( dirName, failedPrefix + controlFileEnding ),
                        "DOWNLOAD_FAILED",
                        failedPrefix,
                        failedPrefix + logFileEnding );

            }
        }
        return iterable;
    }

}
