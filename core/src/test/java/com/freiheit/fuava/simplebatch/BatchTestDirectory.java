package com.freiheit.fuava.simplebatch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freiheit.fuava.simplebatch.util.FileUtils;

public class BatchTestDirectory {

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
        testDirDownloads = testDirBase.resolve( "downloads" );
        testDirProcessing = testDirBase.resolve( "processing" );
        testDirArchive = testDirBase.resolve( "archive" );
        testDirFails = testDirBase.resolve( "fails" );
    }

    public void cleanup() {
        try {
            FileUtils.deleteDirectoryRecursively( testDirBase );
        } catch ( final IOException e ) {
            LOG.error( "Couldn't delete directory " + testDirBase );
        }
    }
}
