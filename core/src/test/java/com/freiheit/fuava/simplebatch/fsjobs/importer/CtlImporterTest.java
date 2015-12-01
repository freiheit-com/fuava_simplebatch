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
package com.freiheit.fuava.simplebatch.fsjobs.importer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.freiheit.fuava.simplebatch.BatchTestDirectory;
import com.freiheit.fuava.simplebatch.MapBasedBatchDownloader;
import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fetch.Fetchers;
import com.freiheit.fuava.simplebatch.fsjobs.downloader.CtlDownloaderJob;
import com.freiheit.fuava.simplebatch.fsjobs.downloader.CtlDownloaderJob.ConfigurationImpl;
import com.freiheit.fuava.simplebatch.logging.JsonLogEntry;
import com.freiheit.fuava.simplebatch.processor.Processors;
import com.freiheit.fuava.simplebatch.processor.StringFileWriterAdapter;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

@Test
public class CtlImporterTest {

    private final Map<Integer, String> data = ImmutableMap.of(
            1, "eins",
            2, "zwei",
            3, "drei",
            4, "vier" );

    @Test
    public void testDownloadAndImport() throws FileNotFoundException, IOException {

        BatchTestDirectory tmp = new BatchTestDirectory( "CtlImporterTest" );

        final AtomicLong counter = new AtomicLong();
        final ResultStatistics downloadResults = new CtlDownloaderJob.Builder<Integer, String>().setConfiguration(
                new ConfigurationImpl().setDownloadDirPath( tmp.getDownloadsDir() ) ).setDownloaderBatchSize( 100 ).setIdsFetcher(
                        Fetchers.iterable( data.keySet() ) ).setDownloader(
                                Processors.retryableBatchedFunction(
                                        new MapBasedBatchDownloader<Integer, String>( data ) ) ).setFileWriterAdapter(
                                                new StringFileWriterAdapter<FetchedItem<Integer>>() {
                                                    @Override
                                                    public String getFileName( final Result<FetchedItem<Integer>, String> result ) {
                                                        return counter.incrementAndGet() + ".tmp";
                                                    }
                                                } ).build().run();

        Assert.assertTrue( downloadResults.isAllSuccess() );
        Assert.assertFalse( downloadResults.isAllFailed() );

        final List<String> importedLines = new ArrayList<String>();

        final ResultStatistics importResults = new CtlImporterJob.Builder<String>().setConfiguration(
                new CtlImporterJob.ConfigurationImpl().setArchivedDirPath( tmp.getArchiveDir() ).setDownloadDirPath(
                        tmp.getDownloadsDir() ).setFailedDirPath( tmp.getFailsDir() ).setProcessingDirPath(
                                tmp.getProcessingDir() ) ).setFileInputStreamReader( new Function<InputStream, Iterable<String>>() {

                                    @Override
                                    public Iterable<String> apply( final InputStream input ) {
                                        try {
                                            try ( BufferedReader ir =
                                                    new BufferedReader( new InputStreamReader( input, Charsets.UTF_8 ) ) ) {
                                                return ImmutableList.of( ir.readLine() );
                                            }
                                        } catch ( final IOException e ) {
                                            throw new RuntimeException( e );
                                        }
                                    }
                                } ).setContentProcessor(
                                        Processors.retryableBatchedFunction( new Function<List<String>, List<String>>() {

                                            @Override
                                            public List<String> apply( final List<String> input ) {
                                                importedLines.addAll( input );
                                                return input;
                                            }
                                        } ) ).build().run();
        Assert.assertTrue( importResults.isAllSuccess() );
        Assert.assertFalse( importResults.isAllFailed() );

        // ignore ordering, because reading of the input files currently is not ordered. 
        Assert.assertEquals( ImmutableSet.copyOf( importedLines ), ImmutableSet.copyOf( data.values() ) );
        Assert.assertTrue( importedLines.size() == 4 );

        // test the contents of one file
        Path file3 = Paths.get( tmp.getArchiveDir(), "3.tmp" );
        List<String> contentLines = Files.readAllLines( file3 );
        Assert.assertEquals( contentLines.size(), 1 );
        Assert.assertEquals( contentLines.get( 0 ), "drei" );

        // test the log contents of one file
        Path log1 = Paths.get( tmp.getArchiveDir(), "1.tmp.log" );
        List<String> logLines = Files.readAllLines( log1 );

        Assert.assertEquals( logLines.size(), 4 );

        JsonLogEntry downloadEnd = JsonLogEntry.fromJson( logLines.get( 0 ) );
        Assert.assertEquals( downloadEnd.getContext(), "write" );
        Assert.assertEquals( downloadEnd.getInput(), "1" );
        Assert.assertEquals( downloadEnd.getEvent(), "end" );
        Assert.assertEquals( downloadEnd.isSuccess(), true );
        Assert.assertNotNull( downloadEnd.getTime() );

        JsonLogEntry importStart = JsonLogEntry.fromJson( logLines.get( 1 ) );
        Assert.assertEquals( importStart.getContext(), "import" );
        Assert.assertEquals( importStart.getEvent(), "start" );
        Assert.assertNotNull( importStart.getTime() );

        JsonLogEntry importItem = JsonLogEntry.fromJson( logLines.get( 2 ) );
        Assert.assertEquals( importItem.getContext(), "import" );
        Assert.assertEquals( importItem.getEvent(), "item" );
        Assert.assertEquals( importItem.getNumber(), 0 );
        Assert.assertEquals( importItem.isSuccess(), true );
        Assert.assertNotNull( importItem.getTime() );

        JsonLogEntry importEnd = JsonLogEntry.fromJson( logLines.get( 3 ) );
        Assert.assertEquals( importEnd.getContext(), "import" );
        Assert.assertEquals( importEnd.getEvent(), "end" );
        Assert.assertEquals( importEnd.isSuccess(), true );
        Assert.assertNotNull( importEnd.getTime() );

        tmp.cleanup();
    }
}