package com.freiheit.fuava.simplebatch.fsjobs.importer;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.freiheit.fuava.simplebatch.BatchTestDirectory;

@Test
public class TestFileMovingPersistence {

    @Test
    public void testMoveBoth() throws Exception {
        final BatchTestDirectory testDir = new BatchTestDirectory( "file-moving" );

        try {

            final String sourceDir = testDir.getDownloadsDir();
            final String targetDir = testDir.getArchiveDir();

            Files.createDirectory( Paths.get( sourceDir ) );
            Files.createDirectory( Paths.get( targetDir ) );

            final String fileA = "/a.txt";
            final String logA = fileA + ".log";
            final String ctlA = fileA + ".ctl";

            final Path sourceFileA = Paths.get( sourceDir, fileA );
            final Path sourceLogA = Paths.get( sourceDir, logA );
            final Path sourceCtlA = Paths.get( sourceDir, ctlA );

            final Path targetFileA = Paths.get( targetDir, fileA );
            final Path targetLogA = Paths.get( targetDir, logA );
            final Path targetCtlA = Paths.get( targetDir, ctlA );

            Files.write( sourceFileA, "Hallo".getBytes( StandardCharsets.UTF_8 ) );
            Files.write( sourceLogA, "logData".getBytes( StandardCharsets.UTF_8 ) );
            Files.write( sourceCtlA, "a".getBytes( StandardCharsets.UTF_8 ) );

            final FileMovingPersistence<Object> fileMovingPersistence = new FileMovingPersistence<>( sourceDir, targetDir, "" );
            final ControlFile ctlFile = new ControlFile( sourceDir, "a.txt", "a.txt.log", new File( sourceDir + "/a.txt.ctl" ) );
            fileMovingPersistence.moveBoth( ctlFile, new File( targetDir ) );        

            Assert.assertFalse( Files.exists( sourceFileA ) );
            Assert.assertTrue( Files.exists( targetFileA ) );

            Assert.assertFalse( Files.exists( sourceLogA ) );
            Assert.assertTrue( Files.exists( targetLogA ) );

            Assert.assertFalse( Files.exists( sourceCtlA ) );
            Assert.assertTrue( Files.exists( targetCtlA ) );
        }
        finally {
            testDir.cleanup();
        }
    }

    @Test
    public void testMoveBothNoLog() throws Exception {
        final BatchTestDirectory testDir = new BatchTestDirectory( "file-moving" );

        try {
            final String sourceDir = testDir.getDownloadsDir();
            final String targetDir = testDir.getArchiveDir();

            Files.createDirectory( Paths.get( sourceDir ) );
            Files.createDirectory( Paths.get( targetDir ) );

            final String fileA = "/a.txt";
            final String logA = fileA + ".log";
            final String ctlA = fileA + ".ctl";

            final Path sourceFileA = Paths.get( sourceDir, fileA );
            final Path sourceLogA = Paths.get( sourceDir, logA );
            final Path sourceCtlA = Paths.get( sourceDir, ctlA );

            final Path targetFileA = Paths.get( targetDir, fileA );
            final Path targetLogA = Paths.get( targetDir, logA );
            final Path targetCtlA = Paths.get( targetDir, ctlA );

            Files.write( sourceFileA, "Hallo".getBytes( StandardCharsets.UTF_8 ) );
            Files.write( sourceCtlA, "a".getBytes( StandardCharsets.UTF_8 ) );

            final FileMovingPersistence<Object> fileMovingPersistence = new FileMovingPersistence<>( sourceDir, targetDir, "" );
            final ControlFile ctlFile = new ControlFile( sourceDir, "a.txt", "a.txt.log", new File( sourceDir + "/a.txt.ctl" ) );
            fileMovingPersistence.moveBoth( ctlFile, new File( targetDir ) );


            Assert.assertFalse( Files.exists( sourceFileA ) );
            Assert.assertTrue( Files.exists( targetFileA ) );

            Assert.assertFalse( Files.exists( sourceLogA ) );
            Assert.assertFalse( Files.exists( targetLogA ) );

            Assert.assertFalse( Files.exists( sourceCtlA ) );
            Assert.assertTrue( Files.exists( targetCtlA ) );

        } finally {
            testDir.cleanup();
        }
    }
}
