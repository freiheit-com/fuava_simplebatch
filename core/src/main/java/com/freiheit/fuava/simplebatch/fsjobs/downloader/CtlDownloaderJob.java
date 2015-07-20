package com.freiheit.fuava.simplebatch.fsjobs.downloader;

import java.util.List;

import com.freiheit.fuava.simplebatch.BatchJob;
import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fetch.Fetcher;
import com.freiheit.fuava.simplebatch.logging.BatchStatisticsLoggingListener;
import com.freiheit.fuava.simplebatch.logging.ItemProgressLoggingListener;
import com.freiheit.fuava.simplebatch.processor.BatchProcessorResult;
import com.freiheit.fuava.simplebatch.processor.ControlFilePersistenceOutputInfo;
import com.freiheit.fuava.simplebatch.processor.FileWriterAdapter;
import com.freiheit.fuava.simplebatch.processor.Processor;
import com.freiheit.fuava.simplebatch.processor.Processors;
import com.freiheit.fuava.simplebatch.result.ProcessingResultListener;
import com.google.common.base.Preconditions;

/**
 * An importer that imports files from the file system, adhering to the control
 * file protocol.
 *
 * @author klas
 *
 * @param <Id>
 *            id which will be used to create the download URL. Could of course
 *            directly be a download URL
 * @param <Data>
 *            the downloaded content, should be easily writeable.
 */
public class CtlDownloaderJob<Id, Data> extends BatchJob<Id, Data> {

    public interface Configuration {

        String getDownloadDirPath();

        String getControlFileEnding();

    }

    public static final String DEFAULT_CONFIG_DOWNLOAD_DIR_PATH = "/tmp/downloading";
    public static final String DEFAULT_CONFIG_CONTROL_FILE_ENDING = ".ctl";

    public static final class ConfigurationImpl implements Configuration {

        private String downloadDirPath = DEFAULT_CONFIG_DOWNLOAD_DIR_PATH;
        private String controlFileEnding = DEFAULT_CONFIG_CONTROL_FILE_ENDING;

        @Override
        public String getDownloadDirPath() {
            return downloadDirPath;
        }

        public ConfigurationImpl setDownloadDirPath( final String path ) {
            this.downloadDirPath = path;
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

    }

    /**
     * Builder for jobs which create one file per downloaded item
     * 
     * @author klas
     *
     * @param <Id>
     *            The type of those items that should be used to really download
     *            the data in the downloader.
     * @param <Data>
     *            The type of the data retrieved by the downloader
     */
    public static final class Builder<Id, Data> extends AbstractBuilder<Id, Data, ControlFilePersistenceOutputInfo> {

        public Builder<Id, Data> setFileWriterAdapter(
                final FileWriterAdapter<FetchedItem<Id>, Data> persistenceAdapter
                ) {
            setFileWriter( Processors.controlledFileWriter(
                    getConfiguration().getDownloadDirPath(),
                    getConfiguration().getControlFileEnding(),
                    persistenceAdapter
                    ) );
            return this;
        }

        @Override
        public Builder<Id, Data> setConfiguration( final Configuration configuration ) {
            super.setConfiguration( configuration );
            return this;
        }

        @Override
        public Builder<Id, Data> setDescription( final String description ) {
            super.setDescription( description );
            return this;
        }

        /**
         * The amount of items from the fetch stage that are put together in a
         * list and passed on to the "Downloader" stage. If your downloader
         * supports batch fetching, you can use this setting to control the
         * amount of items in one batch.
         */
        @Override
        public Builder<Id, Data> setDownloaderBatchSize(
                final int processingBatchSize ) {
            super.setDownloaderBatchSize( processingBatchSize );
            return this;
        }

        /**
         * Fetches the Ids of the documents to download.
         */
        @Override
        public Builder<Id, Data> setIdsFetcher(
                final Fetcher<Id> idsFetcher ) {
            super.setIdsFetcher( idsFetcher );
            return this;
        }

        /**
         * Uses the Ids to download the data.
         */
        @Override
        public Builder<Id, Data> setDownloader(
                final Processor<FetchedItem<Id>, Id, Data> byIdsFetcher ) {
            super.setDownloader( byIdsFetcher );
            return this;
        }

        @Override
        public Builder<Id, Data> addListener(
                final ProcessingResultListener<Id, ControlFilePersistenceOutputInfo> listener ) {
            super.addListener( listener );
            return this;
        }

        @Override
        public Builder<Id, Data> removeListener(
                final ProcessingResultListener<Id, ControlFilePersistenceOutputInfo> listener ) {
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
     * @param <Id>
     * @param <Data>
     */
    public static final class BatchFileWritingBuilder<Id, Data> extends
            AbstractBuilder<Id, Data, BatchProcessorResult<ControlFilePersistenceOutputInfo>> {

        public BatchFileWritingBuilder<Id, Data> setBatchFileWriterAdapter(
                final FileWriterAdapter<List<FetchedItem<Id>>, List<Data>> persistenceAdapter
                ) {
            setFileWriter( Processors.controlledBatchFileWriter(
                    getConfiguration().getDownloadDirPath(),
                    getConfiguration().getControlFileEnding(),
                    persistenceAdapter
                    ) );
            return this;
        }

        @Override
        public BatchFileWritingBuilder<Id, Data> setConfiguration( final Configuration configuration ) {
            super.setConfiguration( configuration );
            return this;
        }

        @Override
        public BatchFileWritingBuilder<Id, Data> setDescription( final String description ) {
            super.setDescription( description );
            return this;
        }

        /**
         * The amount of items from the fetch stage that are put together in a
         * list and passed on to the "Downloader" stage. If your downloader
         * supports batch fetching, you can use this setting to control the
         * amount of items in one batch.
         */
        @Override
        public BatchFileWritingBuilder<Id, Data> setDownloaderBatchSize(
                final int processingBatchSize ) {
            super.setDownloaderBatchSize( processingBatchSize );
            return this;
        }

        /**
         * Fetches the Ids of the documents to download.
         */
        @Override
        public BatchFileWritingBuilder<Id, Data> setIdsFetcher(
                final Fetcher<Id> idsFetcher ) {
            super.setIdsFetcher( idsFetcher );
            return this;
        }

        /**
         * Uses the Ids to download the data.
         */
        @Override
        public BatchFileWritingBuilder<Id, Data> setDownloader(
                final Processor<FetchedItem<Id>, Id, Data> byIdsFetcher ) {
            super.setDownloader( byIdsFetcher );
            return this;
        }

        @Override
        public BatchFileWritingBuilder<Id, Data> addListener(
                final ProcessingResultListener<Id, BatchProcessorResult<ControlFilePersistenceOutputInfo>> listener ) {
            super.addListener( listener );
            return this;
        }

        @Override
        public BatchFileWritingBuilder<Id, Data> removeListener(
                final ProcessingResultListener<Id, BatchProcessorResult<ControlFilePersistenceOutputInfo>> listener ) {
            super.removeListener( listener );
            return this;
        }

    }

    public static abstract class AbstractBuilder<Id, Data, ProcessingResult> {
        private static final String LOG_NAME_BATCH = "ITEMS DOWNLOADED";
        private static final String LOG_NAME_ITEM = "ITEM";
        private final BatchJob.Builder<Id, ProcessingResult> builder = BatchJob.builder();
        private Processor<FetchedItem<Id>, Data, ProcessingResult> fileWriter;
        private Processor<FetchedItem<Id>, Id, Data> downloader;

        private Configuration configuration;

        public AbstractBuilder() {

        }

        public AbstractBuilder<Id, Data, ProcessingResult> setConfiguration( final Configuration configuration ) {
            this.configuration = configuration;
            return this;
        }

        public Configuration getConfiguration() {
            return configuration;
        }

        public AbstractBuilder<Id, Data, ProcessingResult> setDescription( final String description ) {
            builder.setDescription( description );
            return this;
        }

        /**
         * The amount of items from the fetch stage that are put together in a
         * list and passed on to the "Downloader" stage. If your downloader
         * supports batch fetching, you can use this setting to control the
         * amount of items in one batch.
         */
        public AbstractBuilder<Id, Data, ProcessingResult> setDownloaderBatchSize(
                final int processingBatchSize ) {
            builder.setProcessingBatchSize( processingBatchSize );
            return this;
        }

        /**
         * Fetches the Ids of the documents to download.
         */
        public AbstractBuilder<Id, Data, ProcessingResult> setIdsFetcher(
                final Fetcher<Id> idsFetcher ) {
            builder.setFetcher( idsFetcher );
            return this;
        }

        /**
         * Uses the Ids to download the data.
         */
        public AbstractBuilder<Id, Data, ProcessingResult> setDownloader(
                final Processor<FetchedItem<Id>, Id, Data> byIdsFetcher ) {
            downloader = byIdsFetcher;
            return this;
        }

        public AbstractBuilder<Id, Data, ProcessingResult> addListener(
                final ProcessingResultListener<Id, ProcessingResult> listener ) {
            builder.addListener( listener );
            return this;
        }

        public AbstractBuilder<Id, Data, ProcessingResult> removeListener(
                final ProcessingResultListener<Id, ProcessingResult> listener ) {
            builder.removeListener( listener );
            return this;
        }

        protected void setFileWriter(
                final Processor<FetchedItem<Id>, Data, ProcessingResult> fileWriter ) {
            this.fileWriter = fileWriter;
        }

        public CtlDownloaderJob<Id, ProcessingResult> build() {
            builder.addListener( new BatchStatisticsLoggingListener<Id, ProcessingResult>( LOG_NAME_BATCH ) );
            builder.addListener( new ItemProgressLoggingListener<Id, ProcessingResult>( LOG_NAME_ITEM ) );
            Preconditions.checkNotNull( fileWriter, "You need to call setFileWriterAdapter first" );
            final Processor<FetchedItem<Id>, Id, ProcessingResult> p = Processors.compose( fileWriter, downloader );
            return new CtlDownloaderJob<Id, ProcessingResult>(
                    builder.getDescription(),
                    builder.getProcessingBatchSize(),
                    builder.getFetcher(),
                    this.configuration == null
                        ? new ConfigurationImpl()
                        : this.configuration,
                    p,
                    builder.getListeners() );
        }

    }

    private CtlDownloaderJob(
            final String description,
            final int processingBatchSize,
            final Fetcher<Id> fetcher,
            final Configuration configuration,
            final Processor<FetchedItem<Id>, Id, Data> persistence,
            final List<ProcessingResultListener<Id, Data>> listeners ) {
        super( description, processingBatchSize, fetcher, persistence, listeners );
    }

}
