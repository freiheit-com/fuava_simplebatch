package com.freiheit.fuava.simplebatch.processor;

import com.freiheit.fuava.simplebatch.fsjobs.importer.ControlFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class ControlFileWriter {
    public static final Logger LOG = LoggerFactory.getLogger( ControlFileWriter.class );

    public static void write( final ControlFile controlFile ) {
        write( controlFile.getControlFile(), controlFile.getStatus(), controlFile.getControlledFileRelPath(), controlFile.getLogFileRelPath(), controlFile.getOriginalControlledFileName() );
    }
    
    public static void write( final Path controlFile, final String status, final Path commonBaseDir, final Path controlledFileName, final Path logFileName ) {
        checkAbsolute( controlFile, "control" );
        checkCommonBase( controlFile, commonBaseDir, "control" );
        checkAbsolute( controlledFileName, "controlled" );
        checkCommonBase( controlledFileName, commonBaseDir, "Controlled" );
        checkAbsolute( logFileName, "log" );
        checkCommonBase( logFileName, commonBaseDir, "Log" );

        final Path controlledFileRelPath = commonBaseDir.relativize( controlledFileName );
        write( controlFile, status, controlledFileRelPath, commonBaseDir.relativize( logFileName), controlledFileRelPath.toString() );
    }
    
    private static void write( final Path controlFile, final String status, final Path controlledFileName, final Path logFileName, final String originalFileName ) {
        checkAbsolute( controlFile, "control" );
        if ( controlledFileName != null && controlledFileName.isAbsolute() ) {
            throw new IllegalArgumentException( "Expected a relative Path for controlled file, but was " + controlledFileName );
        }
        if ( logFileName.isAbsolute() ) {
            throw new IllegalArgumentException( "Expected a relative Path for log file, but was " + logFileName );
        }
        final String failCtlContent = "#!VERSION=1\n" +
                "status=" + status + "\n" +
                "file=" + ( controlledFileName == null ? "" : controlledFileName.toString() ) + "\n" +
                "originalFileName=" + originalFileName + "\n" +
                "log=" + logFileName.toString();
        try {
            Files.write( controlFile, failCtlContent.getBytes("UTF-8"), StandardOpenOption.CREATE );
        } catch ( final IOException e ) {
            LOG.error( e.getMessage() );
        }
    }

    private static void checkAbsolute( final Path path, final String name ) {
        if ( !path.isAbsolute() ) {
            throw new IllegalArgumentException( "Expected an absolute Path for " + name + " file, but was " + path );
        }
    }

    private static void checkCommonBase( final Path path, final Path commonBase, final String name ) {
        if ( !path.startsWith( commonBase ) ) {
            throw new IllegalArgumentException( name + " file is not in the common base dir [" + commonBase + "], instead it is " + path );
        }
    }

}
