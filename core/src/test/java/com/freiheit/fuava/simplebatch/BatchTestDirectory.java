package com.freiheit.fuava.simplebatch;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freiheit.fuava.simplebatch.util.FileUtils;

public class BatchTestDirectory {

    private static final Logger LOG = LoggerFactory.getLogger( BatchTestDirectory.class );

    private final String testDirBase;
    private final String testDirDownloads;
    private final String testDirProcessing;
    private final String testDirArchive;
    private final String testDirFails;

    public String getTestDirBase() {
        return testDirBase;
    }

    public String getDownloadsDir() {
        return testDirDownloads;
    }

    public String getProcessingDir() {
        return testDirProcessing;
    }

    public String getArchiveDir() {
        return testDirArchive;
    }

    public String getFailsDir() {
        return testDirFails;
    }

    public BatchTestDirectory( String dirName ) {
        testDirBase = "/tmp/" + dirName + "/" + System.currentTimeMillis();
        testDirDownloads = testDirBase + "/downloads/";
        testDirProcessing = testDirBase + "/processing/";
        testDirArchive = testDirBase + "/archive/";
        testDirFails = testDirBase + "/fails/";

    }

    public void cleanup() {
        final File baseDir = new File( testDirBase );
        try {
            FileUtils.deleteDirectoryRecursively( baseDir );
        } catch ( IOException e ) {
            LOG.error( "Couldn't delete directory " + baseDir );
        }
    }
}
