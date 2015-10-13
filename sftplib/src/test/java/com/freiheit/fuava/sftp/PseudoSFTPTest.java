package com.freiheit.fuava.sftp;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
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
import com.freiheit.fuava.simplebatch.processor.ControlFilePersistenceOutputInfo;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;
import com.freiheit.fuava.simplebatch.util.FileUtils;
import com.google.common.collect.ImmutableMap;

@Test
public class PseudoSFTPTest {

    @Test
    public void testPseudoSFTP() throws IOException {
        final String localTestDir = Files.createTempDirectory( "simplebatch_sftplib-pseudotest" ).toFile().getAbsolutePath();
        final CtlDownloaderJob.Configuration localConfig = new CtlDownloaderJob.Configuration() {

            @Override
            public String getDownloadDirPath() {
                return localTestDir;
            }

            @Override
            public String getControlFileEnding() {
                return ".ctl";
            }
            
        };

        final ImmutableMap<String, TestFolder<String>> initialState = ImmutableMap.<String, TestFolder<String>> builder().put(
                "/incoming",
                new TestFolder<String>(
                        ImmutableMap.<String, String> builder()
                        .put( "test_pseudo_21341234", "{name:'pseudojson'}" )
                        .put( "test_pseudo_21341234.ok", "" )
                        .build() 
                ) 
            ).build();
        
        //prepare 'remote' state

        final InMemoryTestRemoteClient<String> client = new InMemoryTestRemoteClient<String>( initialState, ( s ) -> new ByteArrayInputStream( s.getBytes() ) );
        final BatchJob<SftpFilename, ControlFilePersistenceOutputInfo> job =
                SftpDownloaderJob.makeDownloaderJob( localConfig, client, new RemoteConfiguration() {

            @Override
            public String getSkippedFolder() {
                return "/skipped";
            }

            @Override
            public String getProcessingFolder() {
                return "/processing";
            }

            @Override
            public String getIncomingFolder() {
                return "/incoming";
            }

            @Override
            public String getArchivedFolder() {
                return "/archived";
            }
        }, new FileType( "test", "pseudo_{1}" ) );

        final ResultStatistics stat = job.run();

        Assert.assertTrue( stat.isAllSuccess() );
        Assert.assertFalse( stat.isAllFailed() );
        Assert.assertEquals( stat.getFetchCounts().getSuccess(), 1 );
        Assert.assertEquals( stat.getProcessingCounts().getSuccess(), 1 );
        Assert.assertEquals( stat.getFetchCounts().getError(), 0 );
        Assert.assertEquals( stat.getProcessingCounts().getError(), 0 );

        final Map<String, TestFolder<String>> finalState = client.getStateCopy();
        assertIsNullOrEmpty( finalState, "/incoming" );
        assertIsNullOrEmpty( finalState, "/skipped" );
        assertIsNullOrEmpty( finalState, FileUtils.getCurrentDateDirPath( "/skipped" ) );
        assertIsNullOrEmpty( finalState, "/processed" );
        assertIsNullOrEmpty( finalState, "/archived" );
        final String archivedDirPath = FileUtils.getCurrentDateDirPath( "/archived" );
        final TestFolder<String> testFolder = finalState.get( archivedDirPath );
        Assert.assertNotNull( testFolder, "Date-Dependend Archived directory '" + archivedDirPath + "' should not  be null" );
        final Set<String> archiveContent = testFolder.getItemKeys();
        Assert.assertEquals( archiveContent.size(), 2 );
        System.out.println( archiveContent );
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
}
