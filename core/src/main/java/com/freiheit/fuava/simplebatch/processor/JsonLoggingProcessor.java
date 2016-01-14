package com.freiheit.fuava.simplebatch.processor;

import java.nio.file.Path;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.logging.JsonLogger;
import com.freiheit.fuava.simplebatch.result.Result;

public class JsonLoggingProcessor<Input>
        implements Processor<FetchedItem<Input>, FilePersistenceOutputInfo, FilePersistenceOutputInfo> {

    private final Path baseDirectory;
    private final String controlFileEnding;
    private final String logFileEnding;

    public JsonLoggingProcessor( final Path dirName, final String controlFileEnding, final String logFileEnding ) {
        this.baseDirectory = dirName;
        this.controlFileEnding = controlFileEnding;
        this.logFileEnding = logFileEnding;
    }

    @Override
    public Iterable<Result<FetchedItem<Input>, FilePersistenceOutputInfo>> process(
            final Iterable<Result<FetchedItem<Input>, FilePersistenceOutputInfo>> iterable ) {
        for ( final Result<FetchedItem<Input>, FilePersistenceOutputInfo> res : iterable ) {
            Path dataFile = null;
            if ( res.isFailed() ) {
                final Path failedDataFileRelPath = JsonLogger.nextFailedDownloadsName();
                dataFile = baseDirectory.resolve( failedDataFileRelPath );
            } else {
                dataFile = res.getOutput().getDataFile();
            }
            final Path logFile = FileUtil.getLogFilePath( dataFile, logFileEnding );
            final Path ctlFile = FileUtil.getControlFilePath( dataFile, controlFileEnding );
            final JsonLogger l = new JsonLogger( logFile );
            final FetchedItem<Input> fetchedItem = res.getInput();
            final Input input = fetchedItem.getValue();
            final String inputStr = input == null
                ? "null"
                : input.toString();
            l.logWriteEnd( inputStr, res.isSuccess(), res.getAllMessages(), fetchedItem.getIdentifier() );
            if ( res.isFailed() ) {

                ControlFileWriter.write(
                        ctlFile,
                        "DOWNLOAD_FAILED",
                        baseDirectory,
                        dataFile,
                        logFile );

            }
        }
        return iterable;
    }

}
