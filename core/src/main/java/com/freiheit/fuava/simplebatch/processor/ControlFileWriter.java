package com.freiheit.fuava.simplebatch.processor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ControlFileWriter {

    public static final Logger LOG = LoggerFactory.getLogger( ControlFileWriter.class );

    public static void write( final Path controlFile, final String status, final Path commonBaseDir, final Path controlledFileName, final Path logFileName ) {
        Preconditions.checkArgument( controlFile.isAbsolute(), "Expected an absolute Path for control file" );
        Preconditions.checkArgument( controlFile.startsWith( commonBaseDir ), "Control File is not in the common Base Dir" );
        Preconditions.checkArgument( controlledFileName.isAbsolute(), "Expected an absolute Path for controlled file" );
        Preconditions.checkArgument( controlledFileName.startsWith( commonBaseDir ), "Controlled File is not in the common Base Dir" );
        Preconditions.checkArgument( logFileName.isAbsolute(), "Expected an absolute Path for control file" );
        Preconditions.checkArgument( logFileName.startsWith( commonBaseDir ), "Log File is not in the common Base Dir" );

        write( controlFile, status, commonBaseDir.relativize( controlledFileName ), commonBaseDir.relativize( logFileName) );
    }
    
    @Deprecated
    public static void write( final Path controlFile, final String status, final Path controlledFileName, final Path logFileName ) {
        final String failCtlContent = "#!VERSION=1\n" +
                "status=" + status + "\n" +
                "file=" + controlledFileName.toString() + "\n" +
                "log=" + logFileName.toString();
        try {
            Files.write( controlFile, failCtlContent.getBytes("UTF-8"), StandardOpenOption.CREATE );
        } catch ( final IOException e ) {
            LOG.error( e.getMessage() );
        }
    }

}
