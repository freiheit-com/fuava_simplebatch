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
import java.util.ArrayList;
import java.util.List;

import com.freiheit.fuava.simplebatch.BatchJob;
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
import com.freiheit.fuava.simplebatch.result.ProcessingResultListener;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;
import com.freiheit.fuava.simplebatch.util.FileUtils;
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

    public interface Configuration {
        String getControlFileEnding();

        String getDownloadDirPath();

        String getArchivedDirPath();

        String getProcessingDirPath();

        String getFailedDirPath();
    }

    public static final class ConfigurationImpl implements Configuration {

        private String downloadDirPath = CtlDownloaderJob.DEFAULT_CONFIG_DOWNLOAD_DIR_PATH;
        private String archivedDirPath = "/tmp/archive";
        private String processingDirPath = "/tmp/processing/";
        private String failedDirPath = "/tmp/failed";
        private String controlFileEnding = CtlDownloaderJob.DEFAULT_CONFIG_CONTROL_FILE_ENDING;

        @Override
        public String getDownloadDirPath() {
            return downloadDirPath;
        }

        public ConfigurationImpl setDownloadDirPath( final String downloadDirPath ) {
            this.downloadDirPath = downloadDirPath;
            return this;
        }

        @Override
        public String getArchivedDirPath() {
            return archivedDirPath;
        }

        public ConfigurationImpl setArchivedDirPath( final String archivedDirPath ) {
            this.archivedDirPath = archivedDirPath;
            return this;
        }

        @Override
        public String getFailedDirPath() {
            return failedDirPath;
        }

        public ConfigurationImpl setFailedDirPath( final String failedDirPath ) {
            this.failedDirPath = failedDirPath;
            return this;
        }

        @Override
        public String getProcessingDirPath() {
            return processingDirPath;
        }

        public ConfigurationImpl setProcessingDirPath( final String processingDirPath ) {
            this.processingDirPath = processingDirPath;
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

        private String downloadDirPath = CtlDownloaderJob.DEFAULT_CONFIG_DOWNLOAD_DIR_PATH;

        private String archivedDirPath = FileUtils.substitutePlaceholder( "/tmp/archive/" + FileUtils.PLACEHOLDER_DATE );
        private String processingDirPath = "/tmp/processing/";
        private String failedDirPath = FileUtils.substitutePlaceholder( "/tmp/failed/" + FileUtils.PLACEHOLDER_DATE );
        private String controlFileEnding = CtlDownloaderJob.DEFAULT_CONFIG_CONTROL_FILE_ENDING;

        @Override
        public String getDownloadDirPath() {
            return downloadDirPath;
        }

        public ConfigurationWithPlaceholderImpl setDownloadDirPath( final String downloadDirPath ) {
            this.downloadDirPath = FileUtils.substitutePlaceholder( downloadDirPath );
            return this;
        }

        @Override
        public String getArchivedDirPath() {
            return archivedDirPath;
        }

        public ConfigurationWithPlaceholderImpl setArchivedDirPath( final String archivedDirPath ) {
            this.archivedDirPath = FileUtils.substitutePlaceholder( archivedDirPath );
            return this;
        }

        @Override
        public String getFailedDirPath() {
            return failedDirPath;
        }

        public ConfigurationWithPlaceholderImpl setFailedDirPath( final String failedDirPath ) {
            this.failedDirPath = FileUtils.substitutePlaceholder( failedDirPath );
            return this;
        }

        @Override
        public String getProcessingDirPath() {
            return processingDirPath;
        }

        public ConfigurationWithPlaceholderImpl setProcessingDirPath( final String processingDirPath ) {
            this.processingDirPath = FileUtils.substitutePlaceholder( processingDirPath );
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
         * @deprecated Use
         *             {@link #setFetchedItemsFileInputStreamReader(Function)}
         *             instead
         */
        @Deprecated
        public Builder<Data> setFileInputStreamReader( final Function<InputStream, Iterable<Data>> documentReader ) {
            final Function<File, Iterable<Result<FetchedItem<Data>, Data>>> fileProcessorFunction =
                    new FileToInputStreamFunction<>( is -> new IterableFetcherWrapper<Data>( documentReader.apply( is ) ) );
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
                                : new IterableFetcherWrapper<Data>( output ) ).success() );
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

            final Processor<FetchedItem<ControlFile>, ControlFile, ResultStatistics> processor =
                    Processors.<FetchedItem<ControlFile>> controlledFileMover( configuration.getProcessingDirPath() )
                    .then( fileReader )
                    .then( Processors.runBatchJobProcessor(
                            item -> item.getValue().getControlledFileName(), 
                            processingBatchSize, 
                                    TimeLoggingProcessor.wrap( "Content", contentProcessor ),
                            contentProcessingListenerFactories 
                        ))
                    .then( new FileMovingPersistence<ResultStatistics>(
                            new File( configuration.getProcessingDirPath() ),
                            new File( configuration.getArchivedDirPath() ),
                            new File( configuration.getFailedDirPath() ) ) );

            return new CtlImporterJob<Data>(
                    description,
                    1 /* process one file at a time, no use for batching */,
                    Fetchers.folderFetcher( this.configuration.getDownloadDirPath(), this.configuration.getControlFileEnding(),
                            new ReadControlFileFunction( this.configuration.getDownloadDirPath() ) ),
                    TimeLoggingProcessor.wrap( "File", processor ),
                    fileProcessingListeners );
        }
    }

    private CtlImporterJob(
            final String description,
            final int processingBatchSize,
            final Fetcher<ControlFile> fetcher,
            final Processor<FetchedItem<ControlFile>, ControlFile, ResultStatistics> processor,
            final List<ProcessingResultListener<ControlFile, ResultStatistics>> listeners ) {
        super( description, processingBatchSize, fetcher, processor, listeners );
    }

}
