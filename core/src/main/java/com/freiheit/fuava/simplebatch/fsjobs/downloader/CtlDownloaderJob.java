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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import com.freiheit.fuava.simplebatch.BatchJob;
import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fetch.Fetcher;
import com.freiheit.fuava.simplebatch.logging.BatchStatisticsLoggingListener;
import com.freiheit.fuava.simplebatch.logging.ItemProgressLoggingListener;
import com.freiheit.fuava.simplebatch.processor.BatchProcessorResult;
import com.freiheit.fuava.simplebatch.processor.ControlFilePersistenceOutputInfo;
import com.freiheit.fuava.simplebatch.processor.FileOutputStreamAdapter;
import com.freiheit.fuava.simplebatch.processor.Processor;
import com.freiheit.fuava.simplebatch.processor.Processors;
import com.freiheit.fuava.simplebatch.processor.TimeLoggingProcessor;
import com.freiheit.fuava.simplebatch.result.ProcessingResultListener;
import com.freiheit.fuava.simplebatch.util.FileUtils;
import com.freiheit.fuava.simplebatch.util.Sysprops;
import com.google.common.base.Preconditions;

/**
 * An importer that imports files from the file system, adhering to the control
 * file protocol.
 *
 * @author klas
 *
 * @param <OriginalInput>
 *            id which will be used to create the download URL. Could of course
 *            directly be a download URL
 * @param <Output>
 *            the downloaded content, should be easily writeable.
 */
public class CtlDownloaderJob<OriginalInput, Output> extends BatchJob<OriginalInput, Output> {
    public static final String LOG_NAME_BATCH = "ITEMS DOWNLOADED";
    public static final String LOG_NAME_ITEM = "ITEM";

    public interface Configuration {

        Path getDownloadDirPath();
 
        default String getControlFileEnding() {
            return ".ctl";
        }

        default String getLogFileEnding() {
            return ".log";
        }
    }

    public static final String DEFAULT_CONFIG_DOWNLOAD_DIR_PATH = "/tmp/downloading";
    public static final String DEFAULT_CONFIG_CONTROL_FILE_ENDING = ".ctl";
    public static final String DEFAULT_CONFIG_LOG_FILE_ENDING = ".log";

    public static final class ConfigurationImpl implements Configuration {

        private Path downloadDirPath = Paths.get( DEFAULT_CONFIG_DOWNLOAD_DIR_PATH );
        private String controlFileEnding = DEFAULT_CONFIG_CONTROL_FILE_ENDING;
        private String logFileEnding = DEFAULT_CONFIG_LOG_FILE_ENDING;

        @Override
        public Path getDownloadDirPath() {
            return downloadDirPath;
        }

        public ConfigurationImpl setDownloadDirPath( final Path path ) {
            this.downloadDirPath = path == null ? null : path.toAbsolutePath();
            return this;
        }

        public ConfigurationImpl setDownloadDirPath( final String path ) {
            setDownloadDirPath(Paths.get( path ));
            return this;
        }

        @Override
        public String getControlFileEnding() {
            return controlFileEnding;
        }

        public ConfigurationImpl setControlFileEnding( final String ending ) {
            this.controlFileEnding = ending;
            return this;
        }

        @Override
        public String getLogFileEnding() {
            return logFileEnding;
        }

        public ConfigurationImpl setLogFileEnding( final String ending ) {
            this.logFileEnding = ending;
            return this;
        }

    }

    public static final class ConfigurationWithPlaceholderImpl implements Configuration {

        private Path downloadDirPath = Paths.get( DEFAULT_CONFIG_DOWNLOAD_DIR_PATH );
        private String controlFileEnding = DEFAULT_CONFIG_CONTROL_FILE_ENDING;

        @Override
        public Path getDownloadDirPath() {
            return downloadDirPath;
        }

        public ConfigurationWithPlaceholderImpl setDownloadDirPath( final String path ) {
            this.downloadDirPath = Paths.get( FileUtils.substitutePlaceholder( path ) ).toAbsolutePath();
            return this;
        }

        @Override
        public String getControlFileEnding() {
            return controlFileEnding;
        }

        public ConfigurationWithPlaceholderImpl setControlFileEnding( final String ending ) {
            this.controlFileEnding = ending;
            return this;
        }

    }

    /**
     * Builder for jobs which create one file per downloaded item
     * 
     * @author klas
     *
     * @param <OriginalInput>
     *            The type of those items that should be used to really download
     *            the data in the downloader.
     * @param <Input>
     *            The type of the data retrieved by the downloader
     */
    public static final class Builder<OriginalInput, Input> extends AbstractBuilder<OriginalInput, Input, ControlFilePersistenceOutputInfo> {

        private FileOutputStreamAdapter<FetchedItem<OriginalInput>, Input> persistenceAdapter;

        public Builder<OriginalInput, Input> setFileWriterAdapter(
                final FileOutputStreamAdapter<FetchedItem<OriginalInput>, Input> persistenceAdapter ) {

            this.persistenceAdapter = persistenceAdapter;
            return this;
        }

        public CtlDownloaderJob<OriginalInput, ControlFilePersistenceOutputInfo> build() {
            final Configuration configuration = getConfiguration();
            Preconditions.checkNotNull( configuration, "Configuration missing" );
            Preconditions.checkNotNull( persistenceAdapter, "File Writer Adapter missing" );

            return build( Processors.controlledFileWriter(
                    configuration.getDownloadDirPath(),
                    configuration.getControlFileEnding(),
                    configuration.getLogFileEnding(),
                    persistenceAdapter ) );
        }

        @Override
        public Builder<OriginalInput, Input> setConfiguration( final Configuration configuration ) {
            super.setConfiguration( configuration );
            return this;
        }

        @Override
        public Builder<OriginalInput, Input> setDescription( final String description ) {
            super.setDescription( description );
            return this;
        }

        @Override
        public Builder<OriginalInput, Input> setParallelFiles( final boolean parallel ) {
            super.setParallelFiles( parallel );
            return this;
        }

        /**
         * The amount of items from the fetch stage that are put together in a
         * list and passed on to the "Downloader" stage. If your downloader
         * supports batch fetching, you can use this setting to control the
         * amount of items in one batch.
         */
        @Override
        public Builder<OriginalInput, Input> setDownloaderBatchSize(
                final int processingBatchSize ) {
            super.setDownloaderBatchSize( processingBatchSize );
            return this;
        }

        /**
         * Fetches the Ids of the documents to download.
         */
        @Override
        public Builder<OriginalInput, Input> setIdsFetcher(
                final Fetcher<OriginalInput> idsFetcher ) {
            super.setIdsFetcher( idsFetcher );
            return this;
        }

        /**
         * Uses the Ids to download the data.
         */
        @Override
        public Builder<OriginalInput, Input> setDownloader(
                final Processor<FetchedItem<OriginalInput>, OriginalInput, Input> byIdsFetcher ) {
            super.setDownloader( byIdsFetcher );
            return this;
        }

        @Override
        public Builder<OriginalInput, Input> addListener(
                final ProcessingResultListener<OriginalInput, ControlFilePersistenceOutputInfo> listener ) {
            super.addListener( listener );
            return this;
        }

        @Override
        public Builder<OriginalInput, Input> removeListener(
                final ProcessingResultListener<OriginalInput, ControlFilePersistenceOutputInfo> listener ) {
            super.removeListener( listener );
            return this;
        }

    }

    /**
     * Builder for jobs that put multiple downloaded items together in one file,
     * called "batch file".
     * 
     * @author klas
     *
     * @param <Input>
     * @param <IntermediateResult>
     */
    public static final class BatchFileWritingBuilder<Input, IntermediateResult> extends
            AbstractBuilder<Input, IntermediateResult, BatchProcessorResult<ControlFilePersistenceOutputInfo>> {

        private FileOutputStreamAdapter<List<FetchedItem<Input>>, List<IntermediateResult>> persistenceAdapter;

        public BatchFileWritingBuilder<Input, IntermediateResult> setBatchFileWriterAdapter(
                final FileOutputStreamAdapter<List<FetchedItem<Input>>, List<IntermediateResult>> persistenceAdapter ) {
            this.persistenceAdapter = persistenceAdapter;
            return this;
        }

        @Override
        public BatchFileWritingBuilder<Input, IntermediateResult> setConfiguration( final Configuration configuration ) {
            super.setConfiguration( configuration );
            return this;
        }

        @Override
        public BatchFileWritingBuilder<Input, IntermediateResult> setDescription( final String description ) {
            super.setDescription( description );
            return this;
        }

        @Override
        public BatchFileWritingBuilder<Input, IntermediateResult> setParallelFiles( final boolean parallel ) {
            super.setParallelFiles( parallel );
            return this;
        }

        /**
         * The amount of items from the fetch stage that are put together in a
         * list and passed on to the "Downloader" stage. If your downloader
         * supports batch fetching, you can use this setting to control the
         * amount of items in one batch.
         */
        @Override
        public BatchFileWritingBuilder<Input, IntermediateResult> setDownloaderBatchSize(
                final int processingBatchSize ) {
            super.setDownloaderBatchSize( processingBatchSize );
            return this;
        }

        /**
         * Fetches the Ids of the documents to download.
         */
        @Override
        public BatchFileWritingBuilder<Input, IntermediateResult> setIdsFetcher(
                final Fetcher<Input> idsFetcher ) {
            super.setIdsFetcher( idsFetcher );
            return this;
        }

        /**
         * Uses the Ids to download the data.
         */
        @Override
        public BatchFileWritingBuilder<Input, IntermediateResult> setDownloader(
                final Processor<FetchedItem<Input>, Input, IntermediateResult> byIdsFetcher ) {
            super.setDownloader( byIdsFetcher );
            return this;
        }

        @Override
        public BatchFileWritingBuilder<Input, IntermediateResult> addListener(
                final ProcessingResultListener<Input, BatchProcessorResult<ControlFilePersistenceOutputInfo>> listener ) {
            super.addListener( listener );
            return this;
        }

        @Override
        public BatchFileWritingBuilder<Input, IntermediateResult> removeListener(
                final ProcessingResultListener<Input, BatchProcessorResult<ControlFilePersistenceOutputInfo>> listener ) {
            super.removeListener( listener );
            return this;
        }

        public CtlDownloaderJob<Input, BatchProcessorResult<ControlFilePersistenceOutputInfo>> build() {
            final Configuration configuration = getConfiguration();
            Preconditions.checkNotNull( configuration, "Configuration missing" );
            Preconditions.checkNotNull( persistenceAdapter, "File Writer Adapter missing" );

            return build( Processors.controlledBatchFileWriter(
                    configuration.getDownloadDirPath(),
                    configuration.getControlFileEnding(),
                    configuration.getLogFileEnding(),
                    persistenceAdapter ) );
        }
    }

    public static abstract class AbstractBuilder<OriginalInput, Input, Output> {
        private final BatchJob.Builder<OriginalInput, Output> builder = BatchJob.builder();

        private Processor<FetchedItem<OriginalInput>, OriginalInput, Input> downloader;

        private Configuration configuration;

        public AbstractBuilder() {
            builder.setParallel( Sysprops.FILE_PROCESSING_PARALLEL );
            builder.setNumParallelThreads( Sysprops.FILE_PROCESSING_NUM_THREADS );
        }

        public AbstractBuilder<OriginalInput, Input, Output> setConfiguration( final Configuration configuration ) {
            this.configuration = configuration;
            return this;
        }

        public Configuration getConfiguration() {
            return configuration;
        }

        public AbstractBuilder<OriginalInput, Input, Output> setDescription( final String description ) {
            builder.setDescription( description );
            return this;
        }

        public AbstractBuilder<OriginalInput, Input, Output> setParallelFiles( final boolean parallel ) {
            builder.setParallel( parallel );
            return this;
        }
        
        public AbstractBuilder<OriginalInput, Input, Output> setNumParallelThreads( final Integer numParallelThreads ) {
            builder.setNumParallelThreads( numParallelThreads );
            return this;
        }
        
        public AbstractBuilder<OriginalInput, Input, Output> setParallelTerminationTimeoutHours( final int num ) {
            builder.setParallelTerminationTimeoutHours( num );
            return this;
        }

        /**
         * The amount of items from the fetch stage that are put together in a
         * list and passed on to the "Downloader" stage. If your downloader
         * supports batch fetching, you can use this setting to control the
         * amount of items in one batch.
         */
        public AbstractBuilder<OriginalInput, Input, Output> setDownloaderBatchSize(
                final int processingBatchSize ) {
            builder.setProcessingBatchSize( processingBatchSize );
            return this;
        }

        /**
         * Fetches the Ids of the documents to download.
         */
        public AbstractBuilder<OriginalInput, Input, Output> setIdsFetcher(
                final Fetcher<OriginalInput> idsFetcher ) {
            builder.setFetcher( idsFetcher );
            return this;
        }

        /**
         * Uses the Ids to download the data.
         */
        public AbstractBuilder<OriginalInput, Input, Output> setDownloader(
                final Processor<FetchedItem<OriginalInput>, OriginalInput, Input> byIdsFetcher ) {
            downloader = byIdsFetcher;
            return this;
        }

        public AbstractBuilder<OriginalInput, Input, Output> addListener(
                final ProcessingResultListener<OriginalInput, Output> listener ) {
            builder.addListener( listener );
            return this;
        }

        public AbstractBuilder<OriginalInput, Input, Output> removeListener(
                final ProcessingResultListener<OriginalInput, Output> listener ) {
            builder.removeListener( listener );
            return this;
        }
        
        /**
         * Set the callback for 'panic' situations like virtual machine errors where it makes no sense to try and continue processing.
         * Default behaviour is, that System.exit() is called.
         * @param panicCallback The callback for panic situations
         * @return this instance for method chaining
         */
        public AbstractBuilder<OriginalInput, Input, Output> setPanicCallback( final PanicCallback panicCallback ) {
            builder.setPanicCallback( panicCallback );
            return this;
        }

        /**
         * Build a Downloader job with full control over the file writing
         * processor. Note that you need to ensure yourself that the intendend
         * protocol (e. g. control file writing) is followed - for example by
         * using Processors.controlledFileWriter() .
         * 
         * @return the job
         */
        public CtlDownloaderJob<OriginalInput, Output> build( final Processor<FetchedItem<OriginalInput>, Input, Output> fileWriter ) {
            Preconditions.checkNotNull( fileWriter, "You need to call setFileWriterAdapter first" );
            Preconditions.checkNotNull( downloader, "You need to set a downloader" );
            final Fetcher<OriginalInput> fetcher = builder.getFetcher();
            Preconditions.checkNotNull( fetcher, "Fetcher missing." );

            builder.addListener( new BatchStatisticsLoggingListener<OriginalInput, Output>( LOG_NAME_BATCH ) );
            builder.addListener( new ItemProgressLoggingListener<OriginalInput, Output>( LOG_NAME_ITEM ) );

            return new CtlDownloaderJob<OriginalInput, Output>(
                    builder.getDescription(),
                    builder.getProcessingBatchSize(),
                    builder.isParallel(),
                    builder.getNumParallelThreads(),
                    builder.getParallelTerminationTimeoutHours(),
                    fetcher,
                    this.configuration == null
                        ? new ConfigurationImpl()
                        : this.configuration,
                    TimeLoggingProcessor.wrap( "File Download", downloader.then( fileWriter ) ),
                    builder.getListeners(),
                    builder.getPanicCallback()
                    );
        }

    }

    private CtlDownloaderJob(
            final String description,
            final int processingBatchSize,
            final boolean parallel,
            final Integer numParallelThreads,
            final int parallelTerminationTimeoutHours,
            final Fetcher<OriginalInput> fetcher,
            final Configuration configuration,
            final Processor<FetchedItem<OriginalInput>, OriginalInput, Output> persistence,
            final List<ProcessingResultListener<OriginalInput, Output>> listeners,
            final PanicCallback panicCallback) {
        super( description, processingBatchSize, parallel, numParallelThreads, parallelTerminationTimeoutHours, fetcher, persistence, true, listeners, panicCallback );
    }
    

}
