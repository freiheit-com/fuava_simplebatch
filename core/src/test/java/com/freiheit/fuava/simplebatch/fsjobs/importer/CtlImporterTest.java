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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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
import com.freiheit.fuava.simplebatch.processor.RetryingProcessor;
import com.freiheit.fuava.simplebatch.processor.StringFileWriterAdapter;
import com.freiheit.fuava.simplebatch.processor.ToProcessingDirMover;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;

@Test
public class CtlImporterTest {

    private final Map<Integer, String> data = ImmutableMap.of(
            1, "eins",
            2, "zwei",
            3, "drei",
            4, "vier" );

    private final Gson GSON = new Gson();

    @Test
    public void testDownloadAndImport() throws FileNotFoundException, IOException {

        final BatchTestDirectory tmp = new BatchTestDirectory( "CtlImporterTest" );

        final AtomicLong counter = new AtomicLong();
        final ResultStatistics downloadResults = new CtlDownloaderJob.Builder<Integer, String>()
                .setConfiguration( new ConfigurationImpl().setDownloadDirPath( tmp.getDownloadsDir() ) )
                .setDownloaderBatchSize( 100 )
                .setIdsFetcher( Fetchers.iterable( data.keySet() ) )
                .setDownloader( new MapBasedBatchDownloader<Integer, String>( data ) )
                .setFileWriterAdapter(
                    new StringFileWriterAdapter<FetchedItem<Integer>>() {
                        @Override
                        public String getFileName( final Result<FetchedItem<Integer>, String> result ) {
                            return counter.incrementAndGet() + ".tmp";
                        }
                    } 
                )
                .build()
                .run();

        Assert.assertTrue( downloadResults.isAllSuccess() );
        Assert.assertFalse( downloadResults.isAllFailed() );

        final List<String> importedLines = new ArrayList<String>();

        final ResultStatistics importResults = new CtlImporterJob.Builder<String>()
                .setConfiguration( new CtlImporterJob.ConfigurationImpl()
                        .setArchivedDirPath( tmp.getArchiveDir() )
                        .setDownloadDirPath( tmp.getDownloadsDir() )
                        .setFailedDirPath( tmp.getFailsDir() )
                        .setProcessingDirPath( tmp.getProcessingDir() ) 
                )
                .setFileInputStreamReader( new Function<InputStream, Iterable<String>>() {

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
                    } 
                )
                .setContentProcessor( new RetryingProcessor<FetchedItem<String>, String, String>() {

                    @Override
                    public List<String> apply( final List<String> input ) {
                        importedLines.addAll( input );
                        return input;
                    }
                } )
                .build()
                .run();
        Assert.assertTrue( importResults.isAllSuccess() );
        Assert.assertFalse( importResults.isAllFailed() );

        // ignore ordering, because reading of the input files currently is not ordered. 
        Assert.assertEquals( ImmutableSet.copyOf( importedLines ), ImmutableSet.copyOf( data.values() ) );
        Assert.assertTrue( importedLines.size() == 4 );

        // test the contents of a control  file
        assertControlFileConsistent( tmp.getArchiveDir(), "3.tmp" );

        // test the contents of one file
        final Path resultDataFile3 = getResultFilePath( tmp , "3.tmp");
        final List<String> contentLines = Files.readAllLines( resultDataFile3 );
        Assert.assertEquals( contentLines.size(), 1 );
        Assert.assertEquals( contentLines.get( 0 ), "drei" );

        // test the log contents of one file
        final Path log1 = getResultFilePath( tmp , "1.tmp.log" );
        final List<String> logLines = Files.readAllLines( log1 );

        Assert.assertEquals( logLines.size(), 4 );

        final JsonLogEntry downloadEnd = parse( logLines.get( 0 ) );
        Assert.assertEquals( downloadEnd.getContext(), "write" );
        Assert.assertEquals( downloadEnd.getInput(), "1" );
        Assert.assertEquals( downloadEnd.getEvent(), "end" );
        Assert.assertEquals( downloadEnd.isSuccess(), true );
        Assert.assertNotNull( downloadEnd.getTime() );

        final JsonLogEntry importStart = parse( logLines.get( 1 ) );
        Assert.assertEquals( importStart.getContext(), "import" );
        Assert.assertEquals( importStart.getEvent(), "start" );
        Assert.assertNotNull( importStart.getTime() );

        final JsonLogEntry importItem = parse( logLines.get( 2 ) );
        Assert.assertEquals( importItem.getContext(), "import" );
        Assert.assertEquals( importItem.getEvent(), "item" );
        Assert.assertEquals( importItem.getNumber(), 0 );
        Assert.assertEquals( importItem.isSuccess(), true );
        Assert.assertNotNull( importItem.getTime() );

        final JsonLogEntry importEnd = parse( logLines.get( 3 ) );
        Assert.assertEquals( importEnd.getContext(), "import" );
        Assert.assertEquals( importEnd.getEvent(), "end" );
        Assert.assertEquals( importEnd.isSuccess(), true );
        Assert.assertNotNull( importEnd.getTime() );

        tmp.cleanup();
    }

    private void assertControlFileConsistent(final Path directoryToReadFrom, final String baseFileName) {
        
        final Path dataFilePath = getResultFilePath( directoryToReadFrom, baseFileName );
        final Path logFilePath = getResultFilePath( directoryToReadFrom, baseFileName + ".log" );
        final Path controlFilePath = getResultFilePath( directoryToReadFrom, baseFileName + ".ctl" );
        
        final ControlFile controlFile = new ReadControlFileFunction( directoryToReadFrom ).apply( controlFilePath );
        
        Assert.assertEquals( controlFile.getControlFile(), controlFilePath );
        Assert.assertEquals( controlFile.getControlledFile(), dataFilePath );
        Assert.assertEquals( controlFile.getLogFile(), logFilePath );
        Assert.assertEquals( controlFile.getOriginalControlledFileName(), baseFileName );
    }

    private Path getResultFilePath( final BatchTestDirectory tmp, final String baseFileName) {
        return getResultFilePath( tmp.getArchiveDir(), baseFileName );
    }
    
    private Path getResultFilePath( final Path dir, final String baseFileName) {
        // The processing should have created a new file name
        return dir.resolve( ToProcessingDirMover.createInstanceIdPrefix( CtlImporterJob.DEFAULT_INSTANCE_ID ) + baseFileName );
    }

    @Test
    public void testImportWithNullItems() throws FileNotFoundException, IOException {

        final BatchTestDirectory tmp = new BatchTestDirectory( "CtlImporterTest" );
        final Map<Integer, String> data = ImmutableMap.of( 1, "test\nshould work again" );

        final ResultStatistics downloadResults = new CtlDownloaderJob.Builder<Integer, String>()
                .setConfiguration(
                        new ConfigurationImpl()
                        .setDownloadDirPath( tmp.getDownloadsDir() ) 
                )
                .setDownloaderBatchSize( 1 )
                .setIdsFetcher( Fetchers.iterable( data.keySet() ) )
                .setDownloader( new MapBasedBatchDownloader<Integer, String>( data ) )
                .setFileWriterAdapter( new StringFileWriterAdapter<FetchedItem<Integer>>() )
                .build()
                .run();

        Assert.assertTrue( downloadResults.isAllSuccess() );
        Assert.assertFalse( downloadResults.isAllFailed() );

        final List<String> importedLines = new ArrayList<String>();

        new CtlImporterJob.Builder<String>()
                .setConfiguration(
                        new CtlImporterJob.ConfigurationImpl()
                        .setArchivedDirPath( tmp.getArchiveDir() )
                        .setDownloadDirPath( tmp.getDownloadsDir() )
                        .setFailedDirPath( tmp.getFailsDir() )
                        .setProcessingDirPath( tmp.getProcessingDir() ) 
                ).setFetchedItemsFileInputStreamReader( input -> {
                         // Override the actual data, enforce a null in the result 
                        final ArrayList<Result<FetchedItem<String>, String>> list = new ArrayList<Result<FetchedItem<String>, String>>();
                        list.add( Result.success( FetchedItem.of( "test", 1, "id:1" ), "test" ) );
                        list.add( Result.failed( FetchedItem.of( null, 2, "id:2" ), "message: null row invalid" ) );
                        list.add( Result.success( FetchedItem.of( "test", 3, "id:3" ), "should work again" ) );
                        return list;
                    }
                ).setContentProcessor( Processors.retryableBatchedFunction( input -> {
                        importedLines.addAll( input );
                        return input;
                    } ) 
                ).build().run();

        Assert.assertEquals( ImmutableSet.copyOf( importedLines ), ImmutableSet.of( "test", "should work again" ) );
        tmp.cleanup();
    }
    
    @Test
    public void testFetchedImport() throws FileNotFoundException, IOException {

        final BatchTestDirectory tmp = new BatchTestDirectory( "CtlImporterTest" );
        final Map<Integer, String> data = ImmutableMap.of( 1, "test\n\nshould work again" );

        final ResultStatistics downloadResults = new CtlDownloaderJob.Builder<Integer, String>()
                .setConfiguration(
                        new ConfigurationImpl()
                        .setDownloadDirPath( tmp.getDownloadsDir() ) 
                )
                .setDownloaderBatchSize( 1 )
                .setIdsFetcher( Fetchers.iterable( data.keySet() ) )
                .setDownloader( new MapBasedBatchDownloader<Integer, String>( data ) )
                .setFileWriterAdapter( new StringFileWriterAdapter<FetchedItem<Integer>>() )
                .build()
                .run();

        Assert.assertTrue( downloadResults.isAllSuccess() );
        Assert.assertFalse( downloadResults.isAllFailed() );

        final List<String> importedLines = new ArrayList<String>();

        new CtlImporterJob.Builder<String>()
                .setConfiguration(
                        new CtlImporterJob.ConfigurationImpl()
                        .setArchivedDirPath( tmp.getArchiveDir() )
                        .setDownloadDirPath( tmp.getDownloadsDir() )
                        .setFailedDirPath( tmp.getFailsDir() )
                        .setProcessingDirPath( tmp.getProcessingDir() ) 
                ).setFileInputStreamReader( input -> {
                         // Override the actual data, enforce a null in the result 
                        final ArrayList<String> list = new ArrayList<String>();
                        list.add( "test" );
                        list.add( null );
                        list.add( "should work again" );
                        return list;
                    }
                ).setContentProcessor( Processors.retryableBatchedFunction( input -> {
                        importedLines.addAll( input );
                        return input;
                    } ) 
                ).build().run();

        Assert.assertEquals( ImmutableSet.copyOf( importedLines ), ImmutableSet.of( "test", "should work again" ) );
        tmp.cleanup();
    }

    @Test
    public void testFetchedImportWithSubdirs() throws FileNotFoundException, IOException {

        final BatchTestDirectory tmp = new BatchTestDirectory( "CtlImporterTest" );
        final Map<Integer, String> data = ImmutableMap.of( 1, "test\n\nshould work again" );

        final ResultStatistics downloadResults = new CtlDownloaderJob.Builder<Integer, String>()
                .setConfiguration(
                        new ConfigurationImpl()
                        .setDownloadDirPath( tmp.getDownloadsDir() ) 
                )
                .setDownloaderBatchSize( 1 )
                .setIdsFetcher( Fetchers.iterable( data.keySet() ) )
                .setDownloader( new MapBasedBatchDownloader<Integer, String>( data ) )
                .setFileWriterAdapter( new StringFileWriterAdapter<FetchedItem<Integer>>() {
                    @Override
                    public String getFileName(final Result<FetchedItem<Integer>,String> result) {
                        final FetchedItem<Integer> input = result.getInput();
                        return "my/sub/" + (input.getNum() % 2 == 0 ? "dir" : "path") + "/" + input.getValue();
                    };
                } )
                .build()
                .run();

        Assert.assertTrue( downloadResults.isAllSuccess() );
        Assert.assertFalse( downloadResults.isAllFailed() );

        final List<String> importedLines = new ArrayList<String>();

        new CtlImporterJob.Builder<String>()
                .setConfiguration(
                        new CtlImporterJob.ConfigurationImpl()
                        .setArchivedDirPath( tmp.getArchiveDir() )
                        .setDownloadDirPath( tmp.getDownloadsDir() )
                        .setFailedDirPath( tmp.getFailsDir() )
                        .setProcessingDirPath( tmp.getProcessingDir() ) 
                ).setFileInputStreamReader( input -> {
                         // Override the actual data, enforce a null in the result 
                        final ArrayList<String> list = new ArrayList<String>();
                        list.add( "test" );
                        list.add( null );
                        list.add( "should work again" );
                        return list;
                    }
                ).setContentProcessor( Processors.retryableBatchedFunction( input -> {
                        importedLines.addAll( input );
                        return input;
                    } ) 
                ).build().run();

        Assert.assertEquals( ImmutableSet.copyOf( importedLines ), ImmutableSet.of( "test", "should work again" ) );
        tmp.cleanup();
    }

    @Test
    public void testFailingDownloader() throws FileNotFoundException, IOException {

        final BatchTestDirectory tmp = new BatchTestDirectory( "CtlImporterTest" );

        final ResultStatistics downloadResults = new CtlDownloaderJob.Builder<Integer, String>()
                .setConfiguration(new ConfigurationImpl().setDownloadDirPath( tmp.getDownloadsDir() ) )
                .setDownloaderBatchSize( 1 ).setIdsFetcher( Fetchers.iterable( data.keySet() ) )
                .setDownloader( Processors.retryableBatchedFunction( new Function<List<Integer>, List<String>>() {
                    @Override
                    public List<String> apply( final List<Integer> intList ) {
                        if ( intList.contains( 3 ) ) {
                            throw new RuntimeException( "TESTING EXCEPTION IN DOWNLOADER" );
                        } else {
                            return intList.stream().map( x -> x.toString() ).collect( Collectors.toList() );
                        }
                    }
                } ) )
                .setFileWriterAdapter( new StringFileWriterAdapter<FetchedItem<Integer>>() )
                .build()
                .run();

        Assert.assertEquals( downloadResults.getProcessingCounts().getError(), 1 );
        Assert.assertEquals( downloadResults.getProcessingCounts().getSuccess(), 3 );

        final List<String> importedLines = new ArrayList<String>();

        new CtlImporterJob.Builder<String>()
            .setConfiguration(new CtlImporterJob.ConfigurationImpl()
                    .setArchivedDirPath( tmp.getArchiveDir() )
                    .setDownloadDirPath(tmp.getDownloadsDir() )
                    .setFailedDirPath( tmp.getFailsDir() )
                    .setProcessingDirPath(tmp.getProcessingDir() ) )
            .setFileInputStreamReader( input -> {
                                    // The actual result doesn't matter for this test
                                    final ArrayList<String> list = new ArrayList<String>();
                                    list.add( "test" );
                                    return list;
                                } )
            .setContentProcessor( Processors.retryableBatchedFunction( input -> {
                                    importedLines.addAll( input );
                                    return input;
                                } ) )
            .build()
            .run();

        // TODO: assert that only our own exception was thrown, nothing else

        Assert.assertEquals( ImmutableSet.copyOf( importedLines ), ImmutableSet.of( "test" ) );
        tmp.cleanup();
    }

    private JsonLogEntry parse( final String logLine ) {
        return GSON.fromJson( logLine, JsonLogEntry.class );
    }
}