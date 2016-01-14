package com.freiheit.fuava.simplebatch.processor;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.freiheit.fuava.simplebatch.BatchTestDirectory;
import com.freiheit.fuava.simplebatch.fsjobs.importer.ControlFile;

public class ControlFileMoverTest {

    @Test
    public void testMoveBoth() throws Exception {
        final BatchTestDirectory testDir = new BatchTestDirectory( "file-moving" );

        try {

            final Path sourceDir = testDir.getDownloadsDir();
            final Path targetDir = testDir.getArchiveDir();

            Files.createDirectory( sourceDir );
            Files.createDirectory( targetDir );

            final String fileA = "a.txt";
            final String logA = fileA + ".log";
            final String ctlA = fileA + ".ctl";

            final Path sourceFileA = sourceDir.resolve( fileA );
            final Path sourceLogA = sourceDir.resolve( logA );
            final Path sourceCtlA = sourceDir.resolve( ctlA );

            final Path targetFileA = targetDir.resolve( fileA );
            final Path targetLogA = targetDir.resolve( logA );
            final Path targetCtlA = targetDir.resolve( ctlA );

            Files.write( sourceFileA, "Hallo".getBytes( StandardCharsets.UTF_8 ) );
            Files.write( sourceLogA, "logData".getBytes( StandardCharsets.UTF_8 ) );
            Files.write( sourceCtlA, "a".getBytes( StandardCharsets.UTF_8 ) );

            final ControlFile ctlFile = new ControlFile( sourceDir, Paths.get( "a.txt" ), Paths.get( "a.txt.log" ), sourceDir.resolve( "a.txt.ctl" ) );
            ControlFileMover.move( ctlFile, sourceDir, targetDir );        

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
            final Path sourceDir = testDir.getDownloadsDir();
            final Path targetDir = testDir.getArchiveDir();

            Files.createDirectory( sourceDir );
            Files.createDirectory( targetDir );

            final String fileA = "a.txt";
            final String logA = fileA + ".log";
            final String ctlA = fileA + ".ctl";

            final Path sourceFileA = sourceDir.resolve( fileA );
            final Path sourceLogA = sourceDir.resolve( logA );
            final Path sourceCtlA = sourceDir.resolve( ctlA );

            final Path targetFileA = targetDir.resolve( fileA );
            final Path targetLogA = targetDir.resolve( logA );
            final Path targetCtlA = targetDir.resolve( ctlA );

            Files.write( sourceFileA, "Hallo".getBytes( StandardCharsets.UTF_8 ) );
            Files.write( sourceCtlA, "a".getBytes( StandardCharsets.UTF_8 ) );

            final ControlFile ctlFile = new ControlFile( sourceDir, Paths.get( "a.txt" ), Paths.get( "a.txt.log" ), sourceDir.resolve( "a.txt.ctl" ) );
            ControlFileMover.move( ctlFile, sourceDir, targetDir );


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
