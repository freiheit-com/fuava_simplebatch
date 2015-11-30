package com.freiheit.fuava.simplebatch.processor;

import java.nio.file.Paths;
import java.util.List;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.logging.BatchJsonLogger;
import com.freiheit.fuava.simplebatch.result.Result;

public class BatchJsonLoggingSuccessListProcessor<Input>
        implements Processor<List<FetchedItem<Input>>, FilePersistenceOutputInfo, FilePersistenceOutputInfo> {

    private String logFileEnding;

    public BatchJsonLoggingSuccessListProcessor( String logFileEnding ) {
        this.logFileEnding = logFileEnding;
    }

    @Override
    public Iterable<Result<List<FetchedItem<Input>>, FilePersistenceOutputInfo>> process(
            Iterable<Result<List<FetchedItem<Input>>, FilePersistenceOutputInfo>> iterable ) {

        for ( Result<List<FetchedItem<Input>>, FilePersistenceOutputInfo> res : iterable ) {
            BatchJsonLogger l = new BatchJsonLogger( Paths.get(
                    res.getOutput().getDataFile().toString() + logFileEnding ) );
            for ( FetchedItem<Input> item : res.getInput() ) {
                l.logWriteEnd( item.getValue().toString(), true );
            }
        }
        return iterable;
    }
}
