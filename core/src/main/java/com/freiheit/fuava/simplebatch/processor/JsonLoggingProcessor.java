package com.freiheit.fuava.simplebatch.processor;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.logging.JsonLogger;
import com.freiheit.fuava.simplebatch.result.Result;

public class JsonLoggingProcessor<Input>
        implements Processor<FetchedItem<Input>, FilePersistenceOutputInfo, FilePersistenceOutputInfo> {

    private final String dirName;
    private final String controlFileEnding;
    private final String logFileEnding;

    public JsonLoggingProcessor( final String dirName, final String controlFileEnding, final String logFileEnding ) {
        this.dirName = dirName;
        this.controlFileEnding = controlFileEnding;
        this.logFileEnding = logFileEnding;
    }

    @Override
    public Iterable<Result<FetchedItem<Input>, FilePersistenceOutputInfo>> process(
            final Iterable<Result<FetchedItem<Input>, FilePersistenceOutputInfo>> iterable ) {
        for ( final Result<FetchedItem<Input>, FilePersistenceOutputInfo> res : iterable ) {
            Path logFile;
            String failedPrefix = "";
            if ( res.isFailed() ) {
                failedPrefix = JsonLogger.nextFailedDownloadsName();
                logFile = Paths.get( dirName, failedPrefix + logFileEnding );
            } else {
                logFile = Paths.get( res.getOutput().getDataFile() + logFileEnding );
            }
            final JsonLogger l = new JsonLogger( logFile );
            final FetchedItem<Input> fetchedItem = res.getInput();
            final Input input = fetchedItem.getValue();
            final String inputStr = input == null
                ? "null"
                : input.toString();
            l.logWriteEnd( inputStr, res.isSuccess(), res.getAllMessages(), fetchedItem.getIdentifier() );
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
