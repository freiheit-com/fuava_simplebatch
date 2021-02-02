package com.freiheit.fuava.simplebatch.fsjobs.importer;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.logging.JsonLogger;
import com.freiheit.fuava.simplebatch.result.ProcessingResultListener;
import com.freiheit.fuava.simplebatch.result.Result;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class ImportContentJsonLoggingListenerFactory<Data>
        implements Function<FetchedItem<ControlFile>, ProcessingResultListener<Data, Data>> {

    final private Path processingDir;

    public ImportContentJsonLoggingListenerFactory( final Path processingDir ) {
        this.processingDir = processingDir;
    }

    @Override
    public ProcessingResultListener<Data, Data> apply( final FetchedItem<ControlFile> fetchedItem ) {
        final Path logFileRelPath = fetchedItem.getValue().getLogFileRelPath();
        final JsonLogger logger = new JsonLogger( processingDir.resolve( logFileRelPath ) );
        return new ProcessingResultListener<Data, Data>() {
            @Override
            public void onProcessingResult( final Result<FetchedItem<Data>, Data> result ) {
                final FetchedItem<Data> fetchedItem = result.getInput();
                final String identifier = fetchedItem == null
                    ? null
                    : fetchedItem.getIdentifier();

                final List<String> messages = new ArrayList<>();
                result.getWarningMessages().forEach( messages::add );
                result.getFailureMessages().forEach( messages::add );

                logger.logImportItem( result.isSuccess(), result.getInput().getNum(),
                        Collections.unmodifiableList( messages ), identifier );
            }
        };
    }
}