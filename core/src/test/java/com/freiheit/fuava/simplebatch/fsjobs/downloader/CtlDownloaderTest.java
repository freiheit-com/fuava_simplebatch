/**
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
package com.freiheit.fuava.simplebatch.fsjobs.downloader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.freiheit.fuava.simplebatch.BatchTestDirectory;
import com.freiheit.fuava.simplebatch.MapBasedBatchDownloader;
import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fetch.Fetchers;
import com.freiheit.fuava.simplebatch.fsjobs.downloader.CtlDownloaderJob.ConfigurationImpl;
import com.freiheit.fuava.simplebatch.logging.JsonLogEntry;
import com.freiheit.fuava.simplebatch.processor.BatchProcessorResult;
import com.freiheit.fuava.simplebatch.processor.ControlFilePersistenceOutputInfo;
import com.freiheit.fuava.simplebatch.processor.FileWriterAdapter;
import com.freiheit.fuava.simplebatch.processor.Processors;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

@Test
public class CtlDownloaderTest {

    private static BatchTestDirectory tmp = new BatchTestDirectory( "CtlDownloaderTest" );

    public static <Input, Output> CtlDownloaderJob.BatchFileWritingBuilder<Input, Output> newTestDownloaderBuilder() {
        return new CtlDownloaderJob.BatchFileWritingBuilder<Input, Output>().setConfiguration(
                new ConfigurationImpl().setDownloadDirPath( tmp.getDownloadsDir() ) );
    }

    @Test
    public void testBatchPersistence() throws FileNotFoundException, IOException {

        final String targetFileName = "batch";
        final File expected = new File( tmp.getDownloadsDir(), targetFileName + "_1" );
        if ( expected.exists() ) {
            expected.delete();
        }
        Assert.assertFalse( expected.exists(), "File was not deleted " );
        final File expectedLog = new File( tmp.getDownloadsDir(), targetFileName + "_1.log" );
        if ( expectedLog.exists() ) {
            expectedLog.delete();
        }
        Assert.assertFalse( expectedLog.exists(), "Log file was not deleted " );

        final Map<Integer, String> data = new LinkedHashMap<Integer, String>();
        data.put( 1, "eins" );
        data.put( 2, "zwei" );
        data.put( 3, "drei" );
        data.put( 4, "vier" );
        data.put( 5, "fünf" );
        data.put( 6, "sechs" );
        final CtlDownloaderJob.BatchFileWritingBuilder<Integer, String> builder = newTestDownloaderBuilder();
        final CtlDownloaderJob<Integer, BatchProcessorResult<ControlFilePersistenceOutputInfo>> downloader =
                builder.setDownloaderBatchSize( 100 )
                // Fetch ids of the data to be downloaded, will be used by the downloader to fetch the data
                .setIdsFetcher( Fetchers.iterable( data.keySet() ) ).setDownloader( Processors.retryableBatchedFunction(
                        new MapBasedBatchDownloader<Integer, String>( data ) ) ).setBatchFileWriterAdapter(
                                new FileWriterAdapter<List<FetchedItem<Integer>>, List<String>>() {
                                    private final String prefix = targetFileName + "_";
                                    private final AtomicLong counter = new AtomicLong();

                                    @Override
                                    public void write( final Writer writer, final List<String> data ) throws IOException {
                                        final String string = Joiner.on( '\n' ).join( data );
                                        writer.write( string );
                                    }

                                    @Override
                                    public String getFileName( final Result<List<FetchedItem<Integer>>, List<String>> result ) {
                                        return prefix + counter.incrementAndGet();
                                    }
                                } ).build();

        final ResultStatistics results = downloader.run();
        Assert.assertTrue( results.isAllSuccess() );
        Assert.assertFalse( results.isAllFailed() );

        Assert.assertTrue( expected.exists(), "batch file was not created" );
        Assert.assertTrue( expectedLog.exists(), "log file was not created" );

        final ImmutableList.Builder<String> resultsBuilder = ImmutableList.builder();
        try ( BufferedReader reader = new BufferedReader( new FileReader( expected ) ) ) {
            while ( reader.ready() ) {
                resultsBuilder.add( reader.readLine() );
            }
        }

        // test the log contents of THE LOG file
        List<String> logLines = Files.readAllLines( Paths.get( expectedLog.toURI() ) );

        Assert.assertEquals( logLines.size(), 6 );

        JsonLogEntry downloadEnd = JsonLogEntry.fromJson( logLines.get( 0 ) );
        Assert.assertEquals( downloadEnd.getContext(), "write" );
        Assert.assertEquals( downloadEnd.getInput(), "1" );
        Assert.assertEquals( downloadEnd.getEvent(), "end" );
        Assert.assertEquals( downloadEnd.isSuccess(), true );
        Assert.assertNotNull( downloadEnd.getTime() );

        JsonLogEntry write3 = JsonLogEntry.fromJson( logLines.get( 2 ) );
        Assert.assertEquals( write3.getContext(), "write" );
        Assert.assertEquals( write3.getInput(), "3" );
        Assert.assertEquals( write3.getEvent(), "end" );
        Assert.assertEquals( write3.isSuccess(), true );
        Assert.assertNotNull( write3.getTime() );

        JsonLogEntry write6 = JsonLogEntry.fromJson( logLines.get( 5 ) );
        Assert.assertEquals( write6.getContext(), "write" );
        Assert.assertEquals( write6.getInput(), "6" );
        Assert.assertEquals( write6.getEvent(), "end" );
        Assert.assertEquals( write6.isSuccess(), true );
        Assert.assertNotNull( write6.getTime() );

        final ImmutableList<String> resultsList = resultsBuilder.build();
        Assert.assertEquals( resultsList, data.values() );

        tmp.cleanup();
    }

    @Test
    public void testPlaceholderConfiguration() {
        final CtlDownloaderJob.ConfigurationWithPlaceholderImpl config =
                new CtlDownloaderJob.ConfigurationWithPlaceholderImpl().setDownloadDirPath( "/test/%(DATE)/DE" );

        final String currentDate = LocalDate.now().format( DateTimeFormatter.BASIC_ISO_DATE );

        Assert.assertEquals( config.getDownloadDirPath(), "/test/" + currentDate + "/DE/" );
    }

    @Test
    public void testSeveralPlaceholderConfiguration() {
        final CtlDownloaderJob.ConfigurationWithPlaceholderImpl config =
                new CtlDownloaderJob.ConfigurationWithPlaceholderImpl().setDownloadDirPath( "/test/%(DATE)/DE/%(DATE)" );

        final String currentDate = LocalDate.now().format( DateTimeFormatter.BASIC_ISO_DATE );

        Assert.assertEquals( config.getDownloadDirPath(), "/test/" + currentDate + "/DE/" + currentDate + "/" );
    }

}
