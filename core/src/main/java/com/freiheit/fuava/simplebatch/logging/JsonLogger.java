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
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class sets up per-file json logging for the importer, downloader etc
 *
 * @author Benjamin Teuber (benjamin.teuber@freiheit.com)
 */
public class JsonLogger {
    private static final Logger LOG = LoggerFactory.getLogger( JsonLogger.class );
    private Path logFile;

    private static final AtomicLong counter = new AtomicLong();

    public static String nextFailedDownloadsName() {
        final String prefix = "" + System.currentTimeMillis();
        final String count = com.google.common.base.Strings.padStart(
                Long.toString( counter.incrementAndGet() ),
                3,
                '0' );
        return prefix + "_" + count + "_" + "failed_downloads";
    }

    public JsonLogger( Path logFile ) {
        this.logFile = logFile;
    }

    public synchronized void log( JsonLogEntry entry ) {
        try {
            String line = entry.toJson() + "\n";
            Files.write( logFile,
                    line.getBytes( "UTF-8" ),
                    StandardOpenOption.APPEND,
                    StandardOpenOption.CREATE );

        } catch ( IOException e ) {
            LOG.error( e.toString() );
        }
    }

    public void logWriteEnd( String input, boolean isSuccess ) {
        log( new JsonLogEntry( "write", "end", isSuccess, null, input ) );

    }

    public void logImportStart() {
        log( new JsonLogEntry( "import", "start", null, null, null ) );
    }

    public void logImportEnd( boolean isSuccess ) {
        log( new JsonLogEntry( "import", "end", isSuccess, null, null ) );
    }

    public void logImportItem( boolean isSuccess, int number ) {
        log( new JsonLogEntry( "import", "item", isSuccess, number, null ) );
    }
}
