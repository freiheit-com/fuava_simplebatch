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
package com.freiheit.fuava.simplebatch.fsjobs.importer;

import com.freiheit.fuava.simplebatch.BatchTestDirectory;
import com.freiheit.fuava.simplebatch.MapBasedBatchDownloader;
import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fetch.Fetchers;
import com.freiheit.fuava.simplebatch.fsjobs.downloader.CtlDownloaderJob;
import com.freiheit.fuava.simplebatch.fsjobs.downloader.CtlDownloaderJob.ConfigurationImpl;
import com.freiheit.fuava.simplebatch.processor.FileWriterAdapter;
import com.freiheit.fuava.simplebatch.processor.Processor;
import com.freiheit.fuava.simplebatch.processor.RetryingProcessor;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;
import com.freiheit.fuava.simplebatch.util.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Test
public class CtlImporterPathAndConcurrencyTest {

    private final Map<Integer, String> data = CollectionUtils.asMap(
            1, "eins",
            2, "zwei",
            3, "drei",
            4, "vier" );

    @DataProvider
    public Object[][] testData() {
        return new Object[][] {
            
            {testCase( "Success" )
                .successProcessor()
                .expect( BatchTestDirectory.ARCHIVE, CollectionUtils.asSet( "inst_01-1.tmp", "inst_01-2.tmp" ) )},
            
            {testCase( "Fail All" )
                    .failAllProcessor()
                    .expect( BatchTestDirectory.FAILS, CollectionUtils.asSet( "inst_01-1.tmp", "inst_01-2.tmp" ) )},
            
            {testCase( "Fail Some" )
                    .failSomeProcessor()
                    .expect( BatchTestDirectory.ARCHIVE, CollectionUtils.asSet( "inst_01-1.tmp", "inst_01-2.tmp" ) )},
            
            {testCase( "Fail Batch")
                .failBatchProcessor()
                .expect( BatchTestDirectory.ARCHIVE, CollectionUtils.asSet( "inst_01-1.tmp", "inst_01-2.tmp" ) )},
            
            {testCase( "Success File Concurrent" )
                    .successProcessor()
                    .numConcurrentFiles( 2 )
                    .numConcurrentData( 1 )
                    .expect( BatchTestDirectory.ARCHIVE, CollectionUtils.asSet( "inst_01-1.tmp", "inst_01-2.tmp" ) )},
            
            {testCase( "Fail All File Concurrent" )
                    .failAllProcessor()
                    .numConcurrentFiles( 2 )
                    .numConcurrentData( 1 )
                    .expect( BatchTestDirectory.FAILS, CollectionUtils.asSet( "inst_01-1.tmp", "inst_01-2.tmp" ) )},
            
            {testCase( "Fail Some File Concurrent" )
                    .failSomeProcessor()
                    .numConcurrentFiles( 2 )
                    .numConcurrentData( 1 )
                    .expect( BatchTestDirectory.ARCHIVE, CollectionUtils.asSet( "inst_01-1.tmp", "inst_01-2.tmp" ) )},
            
            {testCase( "Fail Batch File Concurrent")
                .failBatchProcessor()
                .numConcurrentFiles( 2 )
                .numConcurrentData( 1 )
                .expect( BatchTestDirectory.ARCHIVE, CollectionUtils.asSet( "inst_01-1.tmp", "inst_01-2.tmp" ) )},
           
            {testCase( "Success File And Data Concurrent" )
                .successProcessor()
                .numConcurrentFiles( 2 )
                .numConcurrentData( 1 )
                .expect( BatchTestDirectory.ARCHIVE, CollectionUtils.asSet( "inst_01-1.tmp", "inst_01-2.tmp" ) )},
                
                {testCase( "Fail All File And Data Concurrent" )
                        .failAllProcessor()
                        .numConcurrentFiles( 2 )
                        .numConcurrentData( 2 )
                        .expect( BatchTestDirectory.FAILS, CollectionUtils.asSet( "inst_01-1.tmp", "inst_01-2.tmp" ) )},
                
                {testCase( "Fail Some File And Data Concurrent" )
                        .failSomeProcessor()
                        .numConcurrentFiles( 2 )
                        .numConcurrentData( 2 )
                        .expect( BatchTestDirectory.ARCHIVE, CollectionUtils.asSet( "inst_01-1.tmp", "inst_01-2.tmp" ) )},
                
                {testCase( "Fail Batch File And Data Concurrent")
                    .failBatchProcessor()
                    .numConcurrentFiles( 2 )
                    .numConcurrentData( 2 )
                    .expect( BatchTestDirectory.ARCHIVE, CollectionUtils.asSet( "inst_01-1.tmp", "inst_01-2.tmp" ) )},
                
                {testCase( "Success Data Concurrent" )
                    .successProcessor()
                    .numConcurrentFiles( 1 )
                    .numConcurrentData( 1 )
                    .expect( BatchTestDirectory.ARCHIVE, CollectionUtils.asSet( "inst_01-1.tmp", "inst_01-2.tmp" ) )},
                
                {testCase( "Fail All Data Concurrent" )
                        .failAllProcessor()
                        .numConcurrentFiles( 1 )
                        .numConcurrentData( 2 )
                        .expect( BatchTestDirectory.FAILS, CollectionUtils.asSet( "inst_01-1.tmp", "inst_01-2.tmp" ) )},
                
                {testCase( "Fail Some Data Concurrent" )
                        .failSomeProcessor()
                        .numConcurrentFiles( 1 )
                        .numConcurrentData( 2 )
                        .expect( BatchTestDirectory.ARCHIVE, CollectionUtils.asSet( "inst_01-1.tmp", "inst_01-2.tmp" ) )},
                
                {testCase( "Fail Batch Data Concurrent")
                    .failBatchProcessor()
                    .numConcurrentFiles( 1 )
                    .numConcurrentData( 2 )
                    .expect( BatchTestDirectory.ARCHIVE, CollectionUtils.asSet( "inst_01-1.tmp", "inst_01-2.tmp" ) )}

        };
    }
    
    @Test(dataProvider="testData")
    public void testDownloadAndImport( final TestCase testCase ) throws FileNotFoundException, IOException {

        final BatchTestDirectory tmp = new BatchTestDirectory( "CtlImporterTest" );
        prepare( tmp );
        
        for (final ExpectedDirState state : testCase.getExpectedDirsAfterDownload()) {
            final Set<String> expectedNames = state.getExpectedNames();
            final Map<String, List<SBFile>> impFiles = listFilesInDir( tmp, state.getDir() );
            Assert.assertEquals( impFiles.keySet(), expectedNames, "Wrong State after Download in " + state.dir   );
        }
        
        createImporterJob( tmp, testCase.getProcessor() )
        .setNumParallelThreadsFiles( testCase.getNumConcurrentFiles() )
        .setParallelFiles( testCase.getNumConcurrentFiles() > 1 )
        .setNumParallelThreadsContent( testCase.getNumConcurrentData() )
        .setParallelContent( testCase.getNumConcurrentData() > 1 )
        .build().run();

        for (final ExpectedDirState state : testCase.getExpectedDirs()) {
            final Set<String> expectedNames = state.getExpectedNames();
            final Map<String, List<SBFile>> impFiles = listFilesInDir( tmp, state.getDir() );
            Assert.assertEquals( impFiles.keySet(), expectedNames, "Wrong State after Import in " + state.dir  );
        }

        tmp.cleanup();
    }


    private static final class FailBatchProcessor extends RetryingProcessor<FetchedItem<String>, String, String> {
        @Override
        public List<String> apply( final List<String> input ) {
            if ( input.size() > 1 ) {
                throw new IllegalArgumentException( "too many items" );
            }
            return input;
        }
    }

    private static final class FailSomeProcessor extends RetryingProcessor<FetchedItem<String>, String, String> {
        @Override
        public List<String> apply( final List<String> input ) {
            for (final String v : input) {
                if ("2".equals(v) || "4".equals( v )) {
                    throw new IllegalArgumentException("Fail all for tests");        
                }
            }
            return input;
        }
    }

    private static final class FailAllProcessor extends RetryingProcessor<FetchedItem<String>, String, String> {
        @Override
        public List<String> apply( final List<String> input ) {
            throw new IllegalArgumentException("Fail all for tests");
        }
    }

    private static final class SuccessProcessor extends RetryingProcessor<FetchedItem<String>, String, String> {
        @Override
        public List<String> apply( final List<String> input ) {
            return input;
        }
    }

    static final class ExpectedDirState {
        private final String dir;
        private final Set<String> expectedNames;
        
        public ExpectedDirState(final String dir, final Set<String> expectedNames) {
            this.dir = dir;
            this.expectedNames = expectedNames;
        }
        
        public String getDir() {
            return dir;
        }
        
        public Set<String> getExpectedNames() {
            return expectedNames;
        }
    }
    
    static final class TestCase {
        private final String name;
        
        private Map<Integer, String> data = Collections.emptyMap();
        private int numConcurrentFiles = 1;
        private int numConcurrentData = 1;
        private int fileBatchSize = 1;
        private final Map<String, ExpectedDirState> expectedDirsAfterImport = new HashMap<String, ExpectedDirState>();
        private final Map<String, ExpectedDirState> expectedDirsAfterDownload = new HashMap<String, ExpectedDirState>();
        private Processor<FetchedItem<String>, String, String> processor = new SuccessProcessor();
        
        public TestCase( final String name ) {
            this.name = name;
        }
        
        public TestCase processor( final Processor<FetchedItem<String>, String, String> processor ) {
            this.processor = processor;
            return this;
        }
        
        public TestCase successProcessor( ) {
            return processor( new SuccessProcessor() );
        }
        public TestCase failAllProcessor( ) {
            return processor( new FailAllProcessor() );
        }
        public TestCase failSomeProcessor( ) {
            return processor( new FailSomeProcessor() );
        }
        public TestCase failBatchProcessor( ) {
            return processor( new FailBatchProcessor() );
        }
        
        public Processor<FetchedItem<String>, String, String> getProcessor() {
            return processor;
        }
        
        public TestCase data(final Map<Integer, String> data) {
            this.data = data;
            return this;
        }
        
        public TestCase numConcurrentData( final int numConcurrentData ) {
            this.numConcurrentData = numConcurrentData;
            return this;
        }
        
        public TestCase numConcurrentFiles( final int numConcurrentFiles ) {
            this.numConcurrentFiles = numConcurrentFiles;
            return this;
        }
        
        public TestCase fileBatchSize( final int fileBatchSize ) {
            this.fileBatchSize = fileBatchSize;
            return this;
        }

        public TestCase expectAfterDownload( final String dir, final Set<String> keys ) {
            expectedDirsAfterDownload.put( dir, new ExpectedDirState( dir, keys ) );
            return this;
        }
        
        public TestCase expect( final String dir, final Set<String> keys ) {
            expectedDirsAfterImport.put( dir, new ExpectedDirState( dir, keys ) );
            return this;
        }
        
        public Map<Integer, String> getData() {
            return data;
        }
        
        public Collection<ExpectedDirState> getExpectedDirs() {
            return expectedDirsAfterImport.values();
        }
        
        public Collection<ExpectedDirState> getExpectedDirsAfterDownload() {
            return expectedDirsAfterDownload.values();
        }
        
        public int getFileBatchSize() {
            return fileBatchSize;
        }
        
        public int getNumConcurrentData() {
            return numConcurrentData;
        }
        
        public int getNumConcurrentFiles() {
            return numConcurrentFiles;
        }
        
        @Override
        public String toString() {
            return name;
        }
    }
    
    static final class SBFile {
        private final String name;
        SBFile(final Path p) {
            name = p.getFileName().toString();
        }
        
        public boolean isCtl() {
            return name.endsWith( ".ctl" );
        }
        
        public boolean isLog() {
            return name.endsWith( ".log" );
        }
        
        public boolean isData() {
            return !isCtl() && !isLog();
        }
        
        public String getDataName() {
            return isData() ? name : name.substring( 0, name.length() - 4 );
        }
        
        @Override
        public String toString() {
            final String prefix = isData() ? "DATA: " : (isLog() ? "LOG: " : "CTL: ");
            return prefix + getDataName();
        }
    }
    
    private TestCase testCase( final String name ) {
        return new TestCase( name )
                .data( data )
                .fileBatchSize( 2 )
                .expectAfterDownload( BatchTestDirectory.ARCHIVE, Collections.emptySet() )
                .expectAfterDownload( BatchTestDirectory.FAILS, Collections.emptySet() )
                .expectAfterDownload( BatchTestDirectory.PROCESSING, Collections.emptySet() )
                .expectAfterDownload( BatchTestDirectory.DOWNLOADS, CollectionUtils.asSet( "1.tmp", "2.tmp" ) )
                // at least one of those will be overridden
                .expect( BatchTestDirectory.ARCHIVE, Collections.emptySet() )
                .expect( BatchTestDirectory.FAILS, Collections.emptySet() )
                .expect( BatchTestDirectory.PROCESSING, Collections.emptySet() )
                .expect( BatchTestDirectory.DOWNLOADS, Collections.emptySet() )
                ;
    }
    
    private void prepare( final BatchTestDirectory tmp ) throws IOException {
        assertIsEmpty(tmp.getArchiveDir());

        final AtomicLong counter = new AtomicLong();
        download( tmp, 2, data, counter );

        assertIsEmpty(tmp.getArchiveDir());
        assertIsEmpty(tmp.getFailsDir());
        assertIsEmpty(tmp.getProcessingDir());
    }

    private void assertIsEmpty( final Path dir ) throws IOException {
        final Map<String, List<SBFile>> files = listFilesInDir( dir );
        Assert.assertTrue( files.isEmpty(), "Directory " + dir + " not empty. It contains files " + files );
    }

    private Map<String, List<SBFile>> listFilesInDir( final BatchTestDirectory cfg, final String dir ) throws IOException {
        return listFilesInDir( cfg.getTestDirBase().resolve( dir ) );
    }
    
    private Map<String, List<SBFile>> listFilesInDir( final Path dir ) throws IOException {
        if ( !dir.toFile().exists() ) {
            return Collections.emptyMap();
        }
        final List<SBFile> files = new ArrayList<>();
        Files.walkFileTree( dir,  new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile( final Path file, final BasicFileAttributes attrs ) throws IOException {
                files.add( new SBFile( file ) );
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory( final Path dir, final IOException exc ) throws IOException {
                return FileVisitResult.CONTINUE;
            }

        });
        return files.stream().collect( Collectors.groupingBy( SBFile::getDataName ) );
    }

    private CtlImporterJob.Builder<String> createImporterJob( final BatchTestDirectory tmp, final Processor<FetchedItem<String>, String, String> processor ) {
        return new CtlImporterJob.Builder<String>()
                .setConfiguration( new CtlImporterJob.ConfigurationImpl()
                        .setArchivedDirPath( tmp.getArchiveDir() )
                        .setDownloadDirPath( tmp.getDownloadsDir() )
                        .setFailedDirPath( tmp.getFailsDir() )
                        .setProcessingDirPath( tmp.getProcessingDir() ) 
                )
                .setFileInputStreamReader( input -> {
                    try {
                        try ( BufferedReader ir =
                                new BufferedReader( new InputStreamReader( input, StandardCharsets.UTF_8 ) ) ) {
                            return Collections.singletonList( ir.readLine() );
                        }
                    } catch ( final IOException e ) {
                        throw new RuntimeException( e );
                    }
                }
                )
                .setContentProcessor( processor )
                ;
    }

    private void download( final BatchTestDirectory tmp, final int batchSize, final Map<Integer, String> data, final AtomicLong counter ) {
        final ResultStatistics downloadResults = new CtlDownloaderJob.BatchFileWritingBuilder<Integer, String>()
                .setConfiguration( new ConfigurationImpl().setDownloadDirPath( tmp.getDownloadsDir() ) )
                .setDownloaderBatchSize( batchSize )
                .setIdsFetcher( Fetchers.iterable( data.keySet() ) )
                .setDownloader( new MapBasedBatchDownloader<Integer, String>( data ) )
                .setBatchFileWriterAdapter( 
                        new FileWriterAdapter<List<FetchedItem<Integer>>, List<String>>() {
                            private final AtomicLong counter = new AtomicLong();

                            @Override
                            public void write( final Writer writer, final List<String> data ) throws IOException {
                                final String string = String.join( "\n", data );
                                writer.write( string );
                            }

                            @Override
                            public Path prependSubdirs(final String filename) {
                                return Paths.get( filename );
                            }

                            @Override
                            public String getFileName( final Result<List<FetchedItem<Integer>>, List<String>> result ) {
                                return counter.incrementAndGet() + ".tmp";
                            }
                        } 
                )
                .build()
                .run();

        Assert.assertTrue( downloadResults.isAllSuccess() );
        Assert.assertFalse( downloadResults.isAllFailed() );
    }

}