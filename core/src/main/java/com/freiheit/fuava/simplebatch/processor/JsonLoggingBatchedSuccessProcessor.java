package com.freiheit.fuava.simplebatch.processor;

import java.nio.file.Paths;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.logging.JsonLogger;
import com.freiheit.fuava.simplebatch.result.Result;

public class JsonLoggingBatchedSuccessProcessor<Input>
        implements Processor<List<FetchedItem<Input>>, FilePersistenceOutputInfo, FilePersistenceOutputInfo> {
    private static final Logger LOG = LoggerFactory.getLogger( JsonLoggingBatchedSuccessProcessor.class );

    private final String logFileEnding;

    public JsonLoggingBatchedSuccessProcessor( final String logFileEnding ) {
        this.logFileEnding = logFileEnding;
    }

    @Override
    public Iterable<Result<List<FetchedItem<Input>>, FilePersistenceOutputInfo>> process(
            final Iterable<Result<List<FetchedItem<Input>>, FilePersistenceOutputInfo>> iterable ) {

        for ( final Result<List<FetchedItem<Input>>, FilePersistenceOutputInfo> res : iterable ) {
            if ( res.isSuccess() ) {
                final FilePersistenceOutputInfo output = res.getOutput();
                if ( output != null ) {
                    final JsonLogger l = new JsonLogger( Paths.get(
                            output.getDataFile().toString() + logFileEnding ) );
                    for ( final FetchedItem<Input> item : res.getInput() ) {
                        l.logWriteEnd( item.getValue().toString(), res.isSuccess(), res.getAllMessages(), item.getIdentifier() );
                    }
                } else {
                    LOG.warn( "No Output for successfully processed Item " + res );
                }
            } else {
                LOG.error( "Cannot log results for failed Item " + res );
            }
        }
        return iterable;
    }
}
