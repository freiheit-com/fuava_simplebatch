package com.freiheit.fuava.simplebatch.fsjobs.importer;

import java.io.File;
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
        BatchTestDirectory testDir = new BatchTestDirectory( "file-moving" );

        try {

            String sourceDir = testDir.getDownloadsDir();
            String targetDir = testDir.getArchiveDir();

            Files.createDirectory( Paths.get( sourceDir ) );
            Files.createDirectory( Paths.get( targetDir ) );

            String fileA = "/a.txt";
            String logA = fileA + ".log";
            String ctlA = fileA + ".ctl";

            Path sourceFileA = Paths.get( sourceDir, fileA );
            Path sourceLogA = Paths.get( sourceDir, logA );
            Path sourceCtlA = Paths.get( sourceDir, ctlA );

            Path targetFileA = Paths.get( targetDir, fileA );
            Path targetLogA = Paths.get( targetDir, logA );
            Path targetCtlA = Paths.get( targetDir, ctlA );

            Files.write( sourceFileA, "Hallo".getBytes() );
            Files.write( sourceLogA, "logData".getBytes() );
            Files.write( sourceCtlA, "a".getBytes() );

            FileMovingPersistence<Object> fileMovingPersistence = new FileMovingPersistence<>( sourceDir, targetDir, "" );
            ControlFile ctlFile = new ControlFile( sourceDir, "a.txt", "a.txt.log", new File( sourceDir + "/a.txt.ctl" ) );
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
        BatchTestDirectory testDir = new BatchTestDirectory( "file-moving" );

        try {
            String sourceDir = testDir.getDownloadsDir();
            String targetDir = testDir.getArchiveDir();

            Files.createDirectory( Paths.get( sourceDir ) );
            Files.createDirectory( Paths.get( targetDir ) );

            String fileA = "/a.txt";
            String logA = fileA + ".log";
            String ctlA = fileA + ".ctl";

            Path sourceFileA = Paths.get( sourceDir, fileA );
            Path sourceLogA = Paths.get( sourceDir, logA );
            Path sourceCtlA = Paths.get( sourceDir, ctlA );

            Path targetFileA = Paths.get( targetDir, fileA );
            Path targetLogA = Paths.get( targetDir, logA );
            Path targetCtlA = Paths.get( targetDir, ctlA );

            Files.write( sourceFileA, "Hallo".getBytes() );
            Files.write( sourceCtlA, "a".getBytes() );

            FileMovingPersistence<Object> fileMovingPersistence = new FileMovingPersistence<>( sourceDir, targetDir, "" );
            ControlFile ctlFile = new ControlFile( sourceDir, "a.txt", "a.txt.log", new File( sourceDir + "/a.txt.ctl" ) );
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
