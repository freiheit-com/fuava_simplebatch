package com.freiheit.fuava.simplebatch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freiheit.fuava.simplebatch.util.FileUtils;

public class BatchTestDirectory {

    public static final String FAILS = "fails";

    public static final String ARCHIVE = "archive";

    public static final String PROCESSING = "processing";

    public static final String DOWNLOADS = "downloads";

    private static final Logger LOG = LoggerFactory.getLogger( BatchTestDirectory.class );

    private final Path testDirBase;
    private final Path testDirDownloads;
    private final Path testDirProcessing;
    private final Path testDirArchive;
    private final Path testDirFails;

    public Path getTestDirBase() {
        return testDirBase;
    }

    public Path getDownloadsDir() {
        return testDirDownloads;
    }

    public Path getProcessingDir() {
        return testDirProcessing;
    }

    public Path getArchiveDir() {
        return testDirArchive;
    }

    public Path getFailsDir() {
        return testDirFails;
    }

    public BatchTestDirectory( final String prefix ) {
        Path base = null;
        try {
            base = Files.createTempDirectory( prefix );
        } catch ( final Exception e ) {
            LOG.error( "Couldn't create temp directory " + prefix );
        }
        testDirBase = base;
        testDirDownloads = testDirBase.resolve( DOWNLOADS );
        testDirProcessing = testDirBase.resolve( PROCESSING );
        testDirArchive = testDirBase.resolve( ARCHIVE );
        testDirFails = testDirBase.resolve( FAILS );
    }

    public void cleanup() {
        try {
            FileUtils.deleteDirectoryRecursively( testDirBase );
        } catch ( final IOException e ) {
            LOG.error( "Couldn't delete directory " + testDirBase );
        }
    }
}
