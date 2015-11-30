package com.freiheit.fuava.simplebatch.processor;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.logging.BatchJsonLogger;
import com.freiheit.fuava.simplebatch.result.Result;

public class BatchJsonLoggingProcessor<Input>
        implements Processor<FetchedItem<Input>, FilePersistenceOutputInfo, FilePersistenceOutputInfo> {

    private String dirName;
    private String controlFileEnding;
    private String logFileEnding;

    public BatchJsonLoggingProcessor( String dirName, String controlFileEnding, String logFileEnding ) {
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
                failedPrefix = BatchJsonLogger.nextFailedDownloadsName();
                logFile = Paths.get( dirName, failedPrefix + logFileEnding );
            } else {
                logFile = Paths.get( res.getOutput().getDataFile() + logFileEnding );
            }
            BatchJsonLogger l = new BatchJsonLogger( logFile );
            l.logWriteEnd( res.getInput().getValue().toString(), res.isSuccess() );
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
