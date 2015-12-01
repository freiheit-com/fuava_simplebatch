package com.freiheit.fuava.simplebatch.processor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControlFileWriter {

    public static final Logger LOG = LoggerFactory.getLogger( ControlFileWriter.class );

    public static void write( Path controlFile, String status, String controlledFileName, String logFileName ) {
        final String failCtlContent = "#!VERSION=1\n" +
                "status=" + status + "\n" +
                "file=" + controlledFileName + "\n" +
                "log=" + logFileName;
        try {
            Files.write( controlFile, failCtlContent.getBytes("UTF-8"), StandardOpenOption.CREATE );
        } catch ( IOException e ) {
            LOG.error( e.getMessage() );
        }
    }

}
