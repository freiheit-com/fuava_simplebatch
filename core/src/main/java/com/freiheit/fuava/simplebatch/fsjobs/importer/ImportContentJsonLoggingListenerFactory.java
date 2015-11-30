package com.freiheit.fuava.simplebatch.fsjobs.importer;

import java.nio.file.Paths;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.logging.BatchJsonLogger;
import com.freiheit.fuava.simplebatch.result.ProcessingResultListener;
import com.freiheit.fuava.simplebatch.result.Result;
import com.google.common.base.Function;

public class ImportContentJsonLoggingListenerFactory<Data>
        implements Function<FetchedItem<ControlFile>, ProcessingResultListener<Data, Data>> {

    final private String processingDir;

    public ImportContentJsonLoggingListenerFactory( String processingDir ) {
        this.processingDir = processingDir;
    }

    @Override
    public ProcessingResultListener<Data, Data> apply( FetchedItem<ControlFile> fetchedItem ) {
        final String logFileName = fetchedItem.getValue().getLogFileName();
        final BatchJsonLogger logger = new BatchJsonLogger( Paths.get( processingDir, logFileName ) );
        return new ProcessingResultListener<Data, Data>() {
            @Override
            public void onProcessingResult( Result<FetchedItem<Data>, Data> result ) {
                logger.logImportItem( result.isSuccess(), result.getInput().getNum() );
            }
        };
    }
}