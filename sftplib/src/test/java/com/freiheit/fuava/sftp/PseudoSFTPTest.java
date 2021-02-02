/*
 * Copyright 2015 freiheit.com technologies gmbh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.freiheit.fuava.sftp;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;
import com.freiheit.fuava.sftp.testclient.InMemoryTestRemoteClient;
import com.freiheit.fuava.sftp.testclient.TestFolder;
import com.freiheit.fuava.sftp.util.FileType;
import com.freiheit.fuava.sftp.util.RemoteConfiguration;
import com.freiheit.fuava.simplebatch.BatchJob;
import com.freiheit.fuava.simplebatch.fsjobs.downloader.CtlDownloaderJob;
import com.freiheit.fuava.simplebatch.logging.BatchStatisticsLoggingListener;
import com.freiheit.fuava.simplebatch.logging.ItemProgressLoggingListener;
import com.freiheit.fuava.simplebatch.logging.JsonLogEntry;
import com.freiheit.fuava.simplebatch.processor.ControlFilePersistenceOutputInfo;
import com.freiheit.fuava.simplebatch.processor.Processors;
import com.freiheit.fuava.simplebatch.processor.TimeLoggingProcessor;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;
import com.freiheit.fuava.simplebatch.util.FileUtils;
import com.google.gson.Gson;

@Test
public class PseudoSFTPTest {

    @Test
    public void testPseudoSFTP() throws IOException {
        final Path localTestDir = Files.createTempDirectory( "simplebatch_sftplib-pseudotest" );
        final String downloadFileName = "test_pseudo_152000_20101010_120000.csv";
        final String downloadFileContent = "{name:'pseudojson'}";
        final CtlDownloaderJob.Configuration localConfig = new CtlDownloaderJob.Configuration() {

            @Override
            public Path getDownloadDirPath() {
                return localTestDir;
            }

            @Override
            public String getControlFileEnding() {
                return ".ctl";
            }

        };

        final Map<String, String> folderContent = new LinkedHashMap<>();
        folderContent.put( downloadFileName, downloadFileContent );
        folderContent.put( "test_pseudo_152000_20101010_120000.ok", "" );

        final HashMap<String, TestFolder<String>> initialState = new HashMap<String, TestFolder<String>>();
        initialState.put( "/incoming", new TestFolder<>( folderContent ) );

        //prepare 'remote' state

        final InMemoryTestRemoteClient<String> client =
                new InMemoryTestRemoteClient<String>( initialState, ( s ) -> new ByteArrayInputStream( s.getBytes() ) );
        final BatchJob<SftpFilename, ControlFilePersistenceOutputInfo> job =
                SftpDownloaderJob.makeOldFilesMovingLatestFileDownloaderJob( localConfig, client,
                        new RemoteConfigurationWithPlaceholderImpl( "/incoming", "/processing", "/skipped/"
                                + FileUtils.PLACEHOLDER_DATE, "/archived/" + FileUtils.PLACEHOLDER_DATE ),
                        new FileType( "test", "_pseudo_" ) );

        final ResultStatistics stat = job.run();

        try {        
            Assert.assertTrue( stat.isAllSuccess() );
            Assert.assertFalse( stat.isAllFailed() );
            Assert.assertEquals( stat.getFetchCounts().getSuccess(), 1 );
            Assert.assertEquals( stat.getProcessingCounts().getSuccess(), 1 );
            Assert.assertEquals( stat.getFetchCounts().getError(), 0 );
            Assert.assertEquals( stat.getProcessingCounts().getError(), 0 );

            final Map<String, TestFolder<String>> finalState = client.getStateCopy();
            assertIsNullOrEmpty( finalState, "/incoming" );
            assertIsNullOrEmpty( finalState, "/skipped/" + LocalDate.now().format( DateTimeFormatter.BASIC_ISO_DATE ) );
            assertIsNullOrEmpty( finalState, "/processed" );
            assertIsNullOrEmpty( finalState, "/archived" );

            final String incomingDirPath = "/incoming/";
            final TestFolder<String> testFolderIncoming = finalState.get( incomingDirPath );
            Assert.assertNotNull( testFolderIncoming, "Incoming directory '" + incomingDirPath + "' should not be null" );
            final Set<String> incomingContent = testFolderIncoming.getItemKeys();
            Assert.assertEquals( incomingContent.size(), 0, "Incoming directory should be empty." );

            final String archivedDirPath = "/archived/" + LocalDate.now().format( DateTimeFormatter.BASIC_ISO_DATE ) + "/";
            final TestFolder<String> testFolder = finalState.get( archivedDirPath );
            Assert.assertNotNull( testFolder, "Date-Dependend Archived directory '" + archivedDirPath + "' should not  be null" );
            final Set<String> archiveContent = testFolder.getItemKeys();
            Assert.assertEquals( archiveContent.size(), 2, "Archived directory should not be empty." );

            final String processingDirPath = "/processing/";
            final TestFolder<String> testFolderProcessing = finalState.get( processingDirPath );
            Assert.assertNotNull( testFolderProcessing, "Processing directory '" + processingDirPath + "' should not  be null" );
            final Set<String> processingContent = testFolderProcessing.getItemKeys();
            Assert.assertEquals( processingContent.size(), 0, "Processing directory should be empty." );

            final Path downloadedFile = Files.newDirectoryStream( localTestDir, "*" + downloadFileName ).iterator().next();
            final String content = Files.readAllLines( downloadedFile ).get( 0 );
            Assert.assertEquals( content, downloadFileContent );

            final Path logFile = Files.newDirectoryStream( localTestDir, "*" + downloadFileName + ".log" ).iterator().next();
            final String logContent = Files.readAllLines( logFile ).get( 0 );

            final JsonLogEntry logEntry = new Gson().fromJson( logContent, JsonLogEntry.class );

            Assert.assertEquals( logEntry.getContext(), "write" );
            Assert.assertEquals( logEntry.getInput(), downloadFileName );
            Assert.assertEquals( logEntry.getEvent(), "end" );
            Assert.assertEquals( logEntry.isSuccess(), true );
            Assert.assertNotNull( logEntry.getTime() );

        } finally {
            FileUtils.deleteDirectoryRecursively( localTestDir.toFile() );
        }
        
        // FIXME: check state -> one success, no skipped dir, one archived subdir with name of current date, nothing in incoming, nothing in processing
    }

    @Test
    public void testPseudoSFTPWithoutProcessingFolder() throws IOException {
        final Path localTestDir = Files.createTempDirectory( "simplebatch_sftplib-pseudotest" );
        final String downloadFileName = "test_pseudo_152000_20101010_120000.csv";
        final String downloadFileContent = "{name:'pseudojson'}";
        final CtlDownloaderJob.Configuration localConfig = new CtlDownloaderJob.Configuration() {

            @Override
            public Path getDownloadDirPath() {
                return localTestDir;
            }

            @Override
            public String getControlFileEnding() {
                return ".ctl";
            }

        };

        final Map<String, String> folderContent = new LinkedHashMap<>();
        folderContent.put( downloadFileName, downloadFileContent );
        folderContent.put( "test_pseudo_152000_20101010_120000.ok", "" );

        final HashMap<String, TestFolder<String>> initialState = new HashMap<String, TestFolder<String>>();
        initialState.put( "/incoming", new TestFolder<>( folderContent ) );

        //prepare 'remote' state

        final InMemoryTestRemoteClient<String> client =
                new InMemoryTestRemoteClient<String>( initialState, ( s ) -> new ByteArrayInputStream( s.getBytes() ) );
        final RemoteConfigurationWithPlaceholderImpl remoteConfiguration =
                new RemoteConfigurationWithPlaceholderImpl( "/incoming", "/skipped/"
                        + FileUtils.PLACEHOLDER_DATE, "/archived/" + FileUtils.PLACEHOLDER_DATE );
        final BatchJob<SftpFilename, ControlFilePersistenceOutputInfo> job =
                makeDownloaderJobWithoutFileMover( localConfig, client, remoteConfiguration );

        final ResultStatistics stat = job.run();

        try {
            Assert.assertTrue( stat.isAllSuccess() );
            Assert.assertFalse( stat.isAllFailed() );
            Assert.assertEquals( stat.getFetchCounts().getSuccess(), 1 );
            Assert.assertEquals( stat.getProcessingCounts().getSuccess(), 1 );
            Assert.assertEquals( stat.getFetchCounts().getError(), 0 );
            Assert.assertEquals( stat.getProcessingCounts().getError(), 0 );

            final Map<String, TestFolder<String>> finalState = client.getStateCopy();
            assertIsNullOrEmpty( finalState, "/incoming" );
            assertIsNullOrEmpty( finalState, "/skipped/" + LocalDate.now().format( DateTimeFormatter.BASIC_ISO_DATE ) );
            assertIsNullOrEmpty( finalState, "/processed" );
            assertIsNullOrEmpty( finalState, "/archived" );

            final String incomingDirPath = "/incoming/";
            final TestFolder<String> testFolderIncoming = finalState.get( incomingDirPath );
            Assert.assertNotNull( testFolderIncoming, "Incoming directory '" + incomingDirPath + "' should not be null" );
            final Set<String> incomingContent = testFolderIncoming.getItemKeys();
            Assert.assertEquals( incomingContent.size(), 2, "Incoming directory should not be empty." );

            final String archivedDirPath = "/archived/" + LocalDate.now().format( DateTimeFormatter.BASIC_ISO_DATE ) + "/";
            final TestFolder<String> testFolderArchived = finalState.get( archivedDirPath );
            Assert.assertNull( testFolderArchived, "Date-Dependend Archived directory '" + archivedDirPath + "' should be null" );

            final String processingDirPath = "/processing/";
            final TestFolder<String> testFolderProcessing = finalState.get( processingDirPath );
            Assert.assertNull( testFolderProcessing, "Processing directory '" + processingDirPath + "' should be null" );

            final Path downloadedFile = Files.newDirectoryStream( localTestDir, "*" + downloadFileName ).iterator().next();
            final String content = Files.readAllLines( downloadedFile ).get( 0 );
            Assert.assertEquals( content, downloadFileContent );

            final Path logFile = Files.newDirectoryStream( localTestDir, "*" + downloadFileName + ".log" ).iterator().next();
            final String logContent = Files.readAllLines( logFile ).get( 0 );

            final JsonLogEntry logEntry = new Gson().fromJson( logContent, JsonLogEntry.class );

            Assert.assertEquals( logEntry.getContext(), "write" );
            Assert.assertEquals( logEntry.getInput(), downloadFileName );
            Assert.assertEquals( logEntry.getEvent(), "end" );
            Assert.assertEquals( logEntry.isSuccess(), true );
            Assert.assertNotNull( logEntry.getTime() );

        } finally {
            FileUtils.deleteDirectoryRecursively( localTestDir.toFile() );
        }

        // FIXME: check state -> one success, no skipped dir, one archived subdir with name of current date, nothing in incoming, nothing in processing
    }

    private void assertIsNullOrEmpty( final Map<String, TestFolder<String>> finalState, final String path ) {
        final TestFolder<String> testFolder = finalState.get( path );
        if ( testFolder == null || testFolder.getItemKeys().isEmpty() ) {
            // good, pass
        } else {
            Assert.fail( "Folder '" + path + "' not empty: " + testFolder.getItemKeys() );
        }
    }

    public static BatchJob<SftpFilename, ControlFilePersistenceOutputInfo> makeDownloaderJobWithoutFileMover(
            final CtlDownloaderJob.Configuration config,
            final RemoteClient client,
            final RemoteConfiguration remoteConfiguration ) {

        final SftpOldFilesMovingLatestFileFetcher fetcher = new SftpOldFilesMovingLatestFileFetcher(
                client,
                remoteConfiguration.getSkippedFolder(),
                remoteConfiguration.getIncomingFolder(),
                new FileType( "test", "_pseudo_" )
        );

        return new BatchJob.Builder<SftpFilename, ControlFilePersistenceOutputInfo>()
                .setFetcher(fetcher )
                .addListener( new BatchStatisticsLoggingListener<>( CtlDownloaderJob.LOG_NAME_BATCH ) )
                .addListener( new ItemProgressLoggingListener<>( CtlDownloaderJob.LOG_NAME_ITEM ) )
                .setProcessor(
                        // downloader
                        TimeLoggingProcessor.wrap( "File Download", Processors.controlledFileWriter(
                                config.getDownloadDirPath(),
                                config.getControlFileEnding(),
                                config.getLogFileEnding(),
                                new SftpDownloadingFileWriterAdapter( client )
                        ) )
                )
                .setProcessingBatchSize( 1 /*No advantage in processing multiple files at once*/ )
                .build();
    }
}
