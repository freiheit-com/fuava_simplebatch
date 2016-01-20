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

import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.freiheit.fuava.simplebatch.BatchJob;
import com.freiheit.fuava.simplebatch.fetch.DownloadDir;
import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fetch.Fetcher;
import com.freiheit.fuava.simplebatch.fetch.Fetchers;
import com.freiheit.fuava.simplebatch.fetch.IterableFetcherWrapper;
import com.freiheit.fuava.simplebatch.fsjobs.downloader.CtlDownloaderJob;
import com.freiheit.fuava.simplebatch.logging.BatchStatisticsLoggingListener;
import com.freiheit.fuava.simplebatch.logging.ImportFileJsonLoggingListener;
import com.freiheit.fuava.simplebatch.logging.ItemProgressLoggingListener;
import com.freiheit.fuava.simplebatch.processor.Processor;
import com.freiheit.fuava.simplebatch.processor.Processors;
import com.freiheit.fuava.simplebatch.processor.TimeLoggingProcessor;
import com.freiheit.fuava.simplebatch.processor.ToProcessingDirMover;
import com.freiheit.fuava.simplebatch.result.ProcessingResultListener;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;
import com.freiheit.fuava.simplebatch.util.FileUtils;
import com.freiheit.fuava.simplebatch.util.Sysprops;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;

/**
 * An importer that imports files from the file system, adhering to the control
 * file protocol.
 * 
 * @author klas
 *
 * @param <ProcessedData>
 */
public class CtlImporterJob<Data> extends BatchJob<ControlFile, ResultStatistics> {
    public static final String DEFAULT_INSTANCE_ID = Sysprops.INSTANCE_NAME;
    private final TimeLoggingProcessor<FetchedItem<Data>, Data, Data> timeLoggedContentProcessor;

    public interface Configuration {

        String getControlFileEnding();

        default String getInstanceId() {
            return DEFAULT_INSTANCE_ID;
        }
        Path getDownloadDirPath();

        Path getArchivedDirPath();

        Path getProcessingDirPath();

        Path getFailedDirPath();
    }

    public static final class ConfigurationImpl implements Configuration {

        private Path downloadDirPath = Paths.get( CtlDownloaderJob.DEFAULT_CONFIG_DOWNLOAD_DIR_PATH );
        private Path archivedDirPath = Paths.get( "/tmp/archive" );
        private Path processingDirPath = Paths.get( "/tmp/processing/" );
        private Path failedDirPath = Paths.get( "/tmp/failed" );
        private String controlFileEnding = CtlDownloaderJob.DEFAULT_CONFIG_CONTROL_FILE_ENDING;

        private String instanceId = DEFAULT_INSTANCE_ID;
        
        @Override
        public String getInstanceId() {
            return instanceId;
        }
        
        public ConfigurationImpl setInstanceId( final String instanceId ) {
            this.instanceId = instanceId;
            return this;
        }
        
        @Override
        public Path getDownloadDirPath() {
            return downloadDirPath;
        }

        public ConfigurationImpl setDownloadDirPath( final String downloadDirPath ) {
            return setDownloadDirPath( Paths.get( downloadDirPath ) );
        }
        
        public ConfigurationImpl setDownloadDirPath( final Path downloadDirPath ) {
            this.downloadDirPath = downloadDirPath == null ? null : downloadDirPath.toAbsolutePath();
            return this;
        }

        @Override
        public Path getArchivedDirPath() {
            return archivedDirPath;
        }

        public ConfigurationImpl setArchivedDirPath( final String archivedDirPath ) {
            return setArchivedDirPath( Paths.get( archivedDirPath ) );
        }
        
        public ConfigurationImpl setArchivedDirPath( final Path archivedDirPath ) {
            this.archivedDirPath = archivedDirPath == null ? null : archivedDirPath.toAbsolutePath();
            return this;
        }

        @Override
        public Path getFailedDirPath() {
            return failedDirPath;
        }

        public ConfigurationImpl setFailedDirPath( final String failedDirPath ) {
            return setFailedDirPath( Paths.get( failedDirPath ) );
        }

        public ConfigurationImpl setFailedDirPath( final Path failedDirPath ) {
            this.failedDirPath = failedDirPath == null ? null : failedDirPath.toAbsolutePath();
            return this;
        }
        
        @Override
        public Path getProcessingDirPath() {
            return processingDirPath;
        }

        public ConfigurationImpl setProcessingDirPath( final String processingDirPath ) {
            return setProcessingDirPath( Paths.get( processingDirPath ) );
        }

        public ConfigurationImpl setProcessingDirPath( final Path processingDirPath ) {
            this.processingDirPath = processingDirPath == null ? null : processingDirPath.toAbsolutePath();
            return this;
        }

        @Override
        public String getControlFileEnding() {
            return controlFileEnding;
        }

        public ConfigurationImpl setControlFileEnding( final String controlFileEnding ) {
            this.controlFileEnding = controlFileEnding;
            return this;
        }

    }

    public static final class ConfigurationWithPlaceholderImpl implements Configuration {

        private Path downloadDirPath = Paths.get( CtlDownloaderJob.DEFAULT_CONFIG_DOWNLOAD_DIR_PATH );

        private Path archivedDirPath = Paths.get( FileUtils.substitutePlaceholder( "/tmp/archive/" + FileUtils.PLACEHOLDER_DATE ) );
        private Path processingDirPath = Paths.get( "/tmp/processing/" );
        private Path failedDirPath = Paths.get( FileUtils.substitutePlaceholder( "/tmp/failed/" + FileUtils.PLACEHOLDER_DATE ) );
        private String controlFileEnding = CtlDownloaderJob.DEFAULT_CONFIG_CONTROL_FILE_ENDING;

        private String instanceId = DEFAULT_INSTANCE_ID;
        
        @Override
        public String getInstanceId() {
            return instanceId;
        }
        
        public ConfigurationWithPlaceholderImpl setInstanceId( final String instanceId ) {
            this.instanceId = instanceId;
            return this;
        }
        
        @Override
        public Path getDownloadDirPath() {
            return downloadDirPath;
        }

        public ConfigurationWithPlaceholderImpl setDownloadDirPath( final String downloadDirPath ) {
            this.downloadDirPath = Paths.get( FileUtils.substitutePlaceholder( downloadDirPath ) ).toAbsolutePath();
            return this;
        }

        @Override
        public Path getArchivedDirPath() {
            return archivedDirPath;
        }

        public ConfigurationWithPlaceholderImpl setArchivedDirPath( final String archivedDirPath ) {
            this.archivedDirPath = Paths.get( FileUtils.substitutePlaceholder( archivedDirPath ) ).toAbsolutePath();
            return this;
        }

        @Override
        public Path getFailedDirPath() {
            return failedDirPath;
        }

        public ConfigurationWithPlaceholderImpl setFailedDirPath( final String failedDirPath ) {
            this.failedDirPath = Paths.get( FileUtils.substitutePlaceholder( failedDirPath ) ).toAbsolutePath();
            return this;
        }

        @Override
        public Path getProcessingDirPath() {
            return processingDirPath;
        }

        public ConfigurationWithPlaceholderImpl setProcessingDirPath( final String processingDirPath ) {
            this.processingDirPath = Paths.get( FileUtils.substitutePlaceholder( processingDirPath ) ).toAbsolutePath();
            return this;
        }

        @Override
        public String getControlFileEnding() {
            return controlFileEnding;
        }

        public ConfigurationWithPlaceholderImpl setControlFileEnding( final String controlFileEnding ) {
            this.controlFileEnding = controlFileEnding;
            return this;
        }

    }

    public static final class Builder<Data> {
        private static final String LOG_NAME_FILE_PROCESSING_ITEM = "FILE";
        private static final String LOG_NAME_FILE_PROCESSING_BATCH = "PROCESSED FILES";
        private static final String LOG_NAME_CONTENT_PROCESSING_ITEM = "CONTENT";
        private static final String LOG_NAME_CONTENT_PROCESSING_BATCH = "PROCESSED CONTENT";
        private Configuration configuration;
        private int processingBatchSize = 1000;

        private final List<ProcessingResultListener<ControlFile, ResultStatistics>> fileProcessingListeners = new ArrayList<>();

        private final List<Function<? super FetchedItem<ControlFile>, ProcessingResultListener<Data, Data>>> contentProcessingListenerFactories =
                new ArrayList<>();
        private Processor<FetchedItem<Data>, Data, Data> contentProcessor;
        private String description;
        private boolean parallelFiles = Sysprops.FILE_PROCESSING_PARALLEL;
        private boolean parallelContent = Sysprops.CONTENT_PROCESSING_PARALLEL;
        private Processor<FetchedItem<ControlFile>, File, Iterable<Result<FetchedItem<Data>, Data>>> fileReader;

        public Builder() {
        }

        /**
         * Controls settings like download directory, processing directory etc.
         */
        public Builder<Data> setConfiguration( final Configuration configuration ) {
            this.configuration = configuration;
            return this;
        }

        public Builder<Data> setDescription( final String description ) {
            this.description = description;
            return this;
        }

        public Builder<Data> setParallelFiles( final boolean parallelFiles ) {
            this.parallelFiles = parallelFiles;
            return this;
        }

        public Builder<Data> setParallelContent( final boolean parallelContent ) {
            this.parallelContent = parallelContent;
            return this;
        }

        /**
         * Controls the number of Data items which will be passed to the content
         * processing stage in one list.
         *
         * Note that your file may contain more items. The partitioning into
         * lists is lazy.
         *
         */
        public Builder<Data> setContentBatchSize( final int processingBatchSize ) {
            this.processingBatchSize = processingBatchSize;
            return this;
        }

        /**
         * The given function is used for each file to convert the contents of
         * that file (accessed by an {@link InputStream}) into an iterable.
         *
         * <p>
         * Note that your Iterable will be processed lazily by creating
         * partitions of size {@link #setContentBatchSize(int)}. After that, it
         * will be passed to the persistence configured in
         * {@link #setContentProcessor(Processor)}.
         * </p>
         *
         * <p>
         * This function is a simple alternative to
         * {@link #setFileProcessor(Processor)}.
         * </p>
         * 
         * @link You should probably use {@link #setFetchedItemsFileInputStreamReader(Function)} instead
         */
        public Builder<Data> setFileInputStreamReader( final Function<InputStream, Iterable<Data>> documentReader ) {
            final Function<File, Iterable<Result<FetchedItem<Data>, Data>>> fileProcessorFunction =
                    new FileToInputStreamFunction<>( is -> IterableFetcherWrapper.wrap( documentReader.apply( is ) ) );
            fileReader = Processors.singleItemFunction( fileProcessorFunction );
            //Result<FetchedItem<Data>, Data>
            return this;
        }

        /**
         * The given function is used for each file to convert the contents of
         * that file (accessed by an {@link InputStream}) into an iterable.
         *
         * <p>
         * Note that your Iterable will be processed lazily by creating
         * partitions of size {@link #setContentBatchSize(int)}. After that, it
         * will be passed to the persistence configured in
         * {@link #setContentProcessor(Processor)}.
         * </p>
         *
         * <p>
         * This function is a simple alternative to
         * {@link #setFetchedItemsFileProcessor(Processor)}.
         * </p>
         */
        public Builder<Data> setFetchedItemsFileInputStreamReader(
                final Function<InputStream, Iterable<Result<FetchedItem<Data>, Data>>> documentReader ) {
            final Function<File, Iterable<Result<FetchedItem<Data>, Data>>> fileProcessorFunction = new FileToInputStreamFunction<>( documentReader );
            fileReader = Processors.singleItemFunction( fileProcessorFunction );
            return this;
        }

        /**
         * Set the processor which is used to convert the File into an iterable
         * of Data items.
         *
         * <p>
         * Note that your Iterable will be processed lazily by creating
         * partitions of size {@link #setContentBatchSize(int)}. After that, it
         * will be passed to the persistence configured in
         * {@link #setContentProcessor(Processor)}.
         * </p>
         * 
         * <p>
         * This function is a powerful alternative to
         * {@link #setFileInputStreamReader(Function)}.
         * </p>
         * 
         * @deprecated Use {@link #setFetchedItemsFileProcessor(Processor)}
         *             instead
         */
        @Deprecated
        public Builder<Data> setFileProcessor( final Processor<FetchedItem<ControlFile>, File, Iterable<Data>> processor ) {
            this.fileReader = new Processor<FetchedItem<ControlFile>, File, Iterable<Result<FetchedItem<Data>, Data>>>() {

                @Override
                public Iterable<Result<FetchedItem<ControlFile>, Iterable<Result<FetchedItem<Data>, Data>>>> process(
                        final Iterable<Result<FetchedItem<ControlFile>, File>> iterable ) {
                    final Iterable<Result<FetchedItem<ControlFile>, Iterable<Data>>> origResult = processor.process( iterable );
                    final ImmutableList.Builder<Result<FetchedItem<ControlFile>, Iterable<Result<FetchedItem<Data>, Data>>>> b =
                            ImmutableList.builder();
                    for ( final Result<FetchedItem<ControlFile>, Iterable<Data>> r : origResult ) {
                        final Iterable<Data> output = r.getOutput();
                        final Result.Builder<FetchedItem<ControlFile>, Iterable<Result<FetchedItem<Data>, Data>>> builder =
                                Result.builder( r );
                        if ( r.isSuccess() ) {
                            b.add( builder.withOutput( output == null
                                ? null
                                : IterableFetcherWrapper.wrap( output ) ).success() );
                        } else {
                            b.add( builder.failed() );
                        }
                    }
                    return b.build();
                }
            };
            return this;
        }

        /**
         * Set the processor which is used to convert the File into an iterable
         * of Data items.
         *
         * <p>
         * Note that your Iterable will be processed lazily by creating
         * partitions of size {@link #setContentBatchSize(int)}. After that, it
         * will be passed to the persistence configured in
         * {@link #setContentProcessor(Processor)}.
         * </p>
         * 
         * <p>
         * This function is a powerful alternative to
         * {@link #setFetchedItemsFileInputStreamReader(Function)}.
         * </p>
         * 
         */
        public Builder<Data> setFetchedItemsFileProcessor(
                final Processor<FetchedItem<ControlFile>, File, Iterable<Result<FetchedItem<Data>, Data>>> processor ) {
            this.fileReader = processor;
            return this;
        }

        /**
         * Controls how to process (or where to store) a batch of the data read
         * from your file.
         *
         * The size of the list which is passed to this persistence is
         * controlled by {@link #setContentBatchSize(int)}
         */
        public Builder<Data> setContentProcessor( final Processor<FetchedItem<Data>, Data, Data> persistence ) {
            contentProcessor = persistence;
            return this;
        }

        public Builder<Data> addFileProcessingListener( final ProcessingResultListener<ControlFile, ResultStatistics> listener ) {
            fileProcessingListeners.add( listener );
            return this;
        }

        public Builder<Data> addContentProcessingListener( final ProcessingResultListener<Data, Data> listener ) {
            contentProcessingListenerFactories.add( Functions.<ProcessingResultListener<Data, Data>> constant( listener ) );
            return this;
        }

        public Builder<Data> addContentProcessingListenerFactory(
                final Function<FetchedItem<ControlFile>, ProcessingResultListener<Data, Data>> listenerFactory ) {
            contentProcessingListenerFactories.add( listenerFactory );
            return this;
        }

        public CtlImporterJob<Data> build() {
            fileProcessingListeners.add( new BatchStatisticsLoggingListener<ControlFile, ResultStatistics>(
                    LOG_NAME_FILE_PROCESSING_BATCH ) );
            fileProcessingListeners.add( new ItemProgressLoggingListener<ControlFile, ResultStatistics>(
                    LOG_NAME_FILE_PROCESSING_ITEM ) );
            fileProcessingListeners.add( new ImportFileJsonLoggingListener(
                    Builder.this.configuration.getDownloadDirPath(),
                    Builder.this.configuration.getArchivedDirPath(),
                    Builder.this.configuration.getFailedDirPath() ) );

            contentProcessingListenerFactories.add(
                    Functions.constant( new BatchStatisticsLoggingListener<Data, Data>( LOG_NAME_CONTENT_PROCESSING_BATCH ) ) );
            contentProcessingListenerFactories.add(
                    Functions.constant( new ItemProgressLoggingListener<Data, Data>( LOG_NAME_CONTENT_PROCESSING_ITEM ) ) );
            contentProcessingListenerFactories.add(
                    new ImportContentJsonLoggingListenerFactory<Data>( Builder.this.configuration.getProcessingDirPath() ) );

            final TimeLoggingProcessor<FetchedItem<Data>, Data, Data> timeLoggedContentProcessor = TimeLoggingProcessor.wrap( "Content Import", contentProcessor );
            final Processor<FetchedItem<ControlFile>, ControlFile, ResultStatistics> processor =
                    Processors.toProcessingDirMover( 
                            configuration.getProcessingDirPath(),
                            configuration.getInstanceId()
                    )
                    .then( fileReader )
                    .then( Processors.runBatchJobProcessor(
                            item -> item.getValue().getControlledFileRelPath().getFileName().toString(), 
                            processingBatchSize, 
                            parallelContent,
                            timeLoggedContentProcessor,
                            contentProcessingListenerFactories 
                        ))
                    .then( Processors.<ResultStatistics> toArchiveDirMover(
                            configuration.getArchivedDirPath(),
                            configuration.getFailedDirPath() ) );

            return new CtlImporterJob<Data>(
                    description,
                    1 /* process one file at a time, no use for batching */,
                    parallelFiles,
                    Fetchers.folderFetcher( 
                            new ReadControlFileFunction( this.configuration.getDownloadDirPath(), this.configuration.getProcessingDirPath() ),
                            // First: process old data from processing for the same instance which was left over when the job got killed
                            new DownloadDir( 
                                    this.configuration.getProcessingDirPath(), 
                                    ToProcessingDirMover.createInstanceIdPrefix( this.configuration.getInstanceId() ) , 
                                    this.configuration.getControlFileEnding() 
                            ),
                            new DownloadDir( this.configuration.getDownloadDirPath(), null, this.configuration.getControlFileEnding() )
                            ),
                    timeLoggedContentProcessor,
                    TimeLoggingProcessor.wrap( "File Import", processor ),
                    fileProcessingListeners );
        }
    }

    private CtlImporterJob(
            final String description,
            final int processingBatchSize,
            final boolean parallel,
            final Fetcher<ControlFile> fetcher,
            final TimeLoggingProcessor<FetchedItem<Data>, Data, Data> timeLoggedContentProcessor,
            final Processor<FetchedItem<ControlFile>, ControlFile, ResultStatistics> processor,
            final List<ProcessingResultListener<ControlFile, ResultStatistics>> listeners ) {
        super( description, processingBatchSize, parallel, fetcher, processor, true, listeners );
        this.timeLoggedContentProcessor = timeLoggedContentProcessor;
    }

    @Override
    public ResultStatistics run() {
        final ResultStatistics stats = super.run();
        // make sure the content counts are logged as well.
        timeLoggedContentProcessor.logFinalCounts();
        return stats;
    }

}
