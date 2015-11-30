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

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class sets up per-file json logging for the importer, downloader etc
 *
 * @author Benjamin Teuber (benjamin.teuber@freiheit.com)
 */
public class BatchJsonLogger {
    private static final Logger LOG = LoggerFactory.getLogger( BatchJsonLogger.class );
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

    public BatchJsonLogger( Path logFile ) {
        this.logFile = logFile;
    }

    public synchronized void log( JSONObject o ) {
        try {
            String line = o.toJSONString() + "\n";
            Files.write( logFile,
                    line.getBytes(),
                    StandardOpenOption.APPEND,
                    StandardOpenOption.CREATE );

        } catch ( IOException e ) {
            LOG.error( e.toString() );
        }
    }

    @SuppressWarnings( "unchecked" )
    public JSONObject entry( String context, String event, Object... args ) {
        JSONObject o = new JSONObject();
        o.put( "time", System.currentTimeMillis() );
        o.put( "context", context );
        o.put( "event", event );
        for ( int i = 0; i + 1 < args.length; i += 2 ) {
            o.put( args[i], args[i + 1] );
        }
        return o;
    }

    public void logWriteEnd( String input, boolean isSuccess ) {
        log( entry( "write", "end", "success", isSuccess, "input", input ) );
    }

    public void logImportStart() {
        log( entry( "import", "start" ) );
    }

    public void logImportEnd( boolean isSuccess ) {
        log( entry( "import", "end", "success", isSuccess ) );
    }

    public void logImportItem( boolean isSuccess, int number ) {
        log( entry( "import", "item", "success", isSuccess, "number", number ) );
    }
}
