package com.freiheit.fuava.simplebatch.logging;

/*
 *
 * Created on Nov 17, 2015 by
 * Benjamin Teuber (benjamin.teuber@freiheit.com)
 */
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;

/**
 * This class sets up per-file json logging for the importer, downloader etc
 *
 * @author Benjamin Teuber (benjamin.teuber@freiheit.com)
 */
public class JsonLogger {
    private static final Logger LOG = LoggerFactory.getLogger( JsonLogger.class );
    private final Path logFile;
    private final Gson gson = new Gson();

    private static final AtomicLong counter = new AtomicLong();

    public static String nextFailedDownloadsName() {
        final String prefix = "" + System.currentTimeMillis();
        final String count = com.google.common.base.Strings.padStart(
                Long.toString( counter.incrementAndGet() ),
                3,
                '0' );
        return prefix + "_" + count + "_" + "failed_downloads";
    }

    public JsonLogger( final Path logFile ) {
        this.logFile = logFile;
    }

    public synchronized void log( final JsonLogEntry entry ) {
        try {
            final String line = gson.toJson( entry ) + "\n";
            Files.write( logFile,
                    line.getBytes( "UTF-8" ),
                    StandardOpenOption.APPEND,
                    StandardOpenOption.CREATE );

        } catch ( final IOException e ) {
            LOG.error( e.toString() );
        }
    }

    public void logWriteEnd( final String input, final boolean isSuccess, final List<String> messages, final String idString ) {
        log( new JsonLogEntry( "write", "end", isSuccess, null, input, messages, idString ) );

    }

    public void logImportStart( final String identifier ) {
        log( new JsonLogEntry( "import", "start", null, null, null, ImmutableList.of(), identifier ) );
    }

    public void logImportEnd( final boolean isSuccess, final List<String> messages, final String identifier ) {
        log( new JsonLogEntry( "import", "end", isSuccess, null, null, messages, identifier ) );
    }

    public void logImportItem( final boolean isSuccess, final int number, final List<String> messages, final String identifier ) {
        log( new JsonLogEntry( "import", "item", isSuccess, number, null, messages, identifier ) );
    }
}
