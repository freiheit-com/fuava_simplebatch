package com.freiheit.fuava.simplebatch.fsjobs.downloader;

import java.util.List;
import java.util.Map;

import com.freiheit.fuava.simplebatch.BatchJob;
import com.freiheit.fuava.simplebatch.fetch.Fetcher;
import com.freiheit.fuava.simplebatch.logging.ProcessingBatchListener;
import com.freiheit.fuava.simplebatch.logging.ProcessingItemListener;
import com.freiheit.fuava.simplebatch.persist.*;
import com.freiheit.fuava.simplebatch.process.Processor;
import com.freiheit.fuava.simplebatch.result.ProcessingResultListener;
import com.google.common.base.Function;
import com.google.common.base.Supplier;

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
public class FSDownloaderJob<Id, Data> extends BatchJob<Id, Data> {

    public interface Configuration extends FilePersistence.Configuration, ControlFilePersistence.Configuration {

    }

    public static final class ConfigurationImpl implements Configuration {

        private String downloadDirPath = "/tmp/downloading";
        private String controlFileEnding = ".ctl";

        @Override
        public String getDownloadDirPath() {
            return downloadDirPath;
        }

        public ConfigurationImpl setDownloadDirPath( String path ) {
            this.downloadDirPath = path;
            return this;
        }

        @Override
        public String getControlFileEnding() {
            return controlFileEnding;
        }

        public ConfigurationImpl setControlFileEnding( String ending ) {
            this.controlFileEnding = ending;
            return this;
        }

    }

    public static final class Builder<Id, Data> {
        private static final String LOG_NAME = "Downloader";
		private final BatchJob.Builder<Id, Data> builder = BatchJob.builder();
        private PersistenceAdapter<Id, Data> persistenceAdapter;
        private Configuration configuration;

        public Builder() {

        }

        public Builder<Id, Data> setConfiguration( Configuration configuration ) {
            this.configuration = configuration;
            return this;
        }

        public Builder<Id, Data> setDownloaderBatchSize(
                int processingBatchSize ) {
            builder.setProcessingBatchSize( processingBatchSize );
            return this;
        }

        /**
         * Fetches the Ids of the documents to download.
         */
        public Builder<Id, Data> setIdsFetcher(
                Fetcher<Id> idsFetcher ) {
            builder.setFetcher( idsFetcher );
            return this;
        }

        /**
         * Fetches the Ids of the documents to download.
         */
        public Builder<Id, Data> setIdsFetcher(
                Iterable<Id> idsFetcher ) {
            builder.setFetcher( idsFetcher );
            return this;
        }

        /**
         * Fetches the Ids of the documents to download.
         */
        public Builder<Id, Data> setIdsFetcher(
                Supplier<Iterable<Id>> idsFetcher ) {
            builder.setFetcher( idsFetcher );
            return this;
        }

        /**
         * Uses the Ids to download the data.
         */
        public Builder<Id, Data> setDownloader(
                Processor<Id, Data> byIdsFetcher ) {
            builder.setProcessor( byIdsFetcher );
            return this;
        }

        /**
         * Uses the Ids to download the data.
         */
        public Builder<Id, Data> setRetryableDownloader(
                Function<List<Id>, Map<Id, Data>> retryableFunction ) {
            builder.setRetryableProcessor( retryableFunction );
            return this;
        }

        /**
         * Uses the Ids to download the data.
         */
        public Builder<Id, Data> setRetryableListDownloader(
                Function<List<Id>, List<Data>> retryableFunction ) {
            builder.setRetryableListProcessor( retryableFunction );
            return this;
        }

        public Builder<Id, Data> addListener(
                ProcessingResultListener<Id, Data> listener ) {
            builder.addListener( listener );
            return this;
        }

        public Builder<Id, Data> removeListener(
                ProcessingResultListener<Id, Data> listener ) {
            builder.removeListener( listener );
            return this;
        }

        public Builder<Id, Data> setFileWriterAdapter( PersistenceAdapter<Id, Data> persistenceAdapter ) {
            this.persistenceAdapter = persistenceAdapter;
            return this;
        }

        public FSDownloaderJob<Id, Data> build() {
            builder.addListener( new ProcessingBatchListener<Id, Data>(LOG_NAME) );
            builder.addListener( new ProcessingItemListener<Id, Data>(LOG_NAME) );
            return new FSDownloaderJob<Id, Data>(
                    builder.getProcessingBatchSize(),
                    builder.getFetcher(),
                    builder.getProcessor(),
                    this.configuration == null
                        ? new ConfigurationImpl()
                        : this.configuration,
                    this.persistenceAdapter == null
                        ? new AbstractStringPersistenceAdapter<Id, Data>() {
                        }
                        : this.persistenceAdapter,
                    builder.getListeners() );
        }

    }

    protected FSDownloaderJob(
            int processingBatchSize,
            Fetcher<Id> fetcher,
            Processor<Id, Data> processor,
            Configuration configuration,
            PersistenceAdapter<Id, Data> persistence,
            List<ProcessingResultListener<Id, Data>> listeners ) {
        super( processingBatchSize, fetcher, processor,
                Persistences.compose(
                        new ControlFilePersistence<Id>( configuration ),
                        new FilePersistence<Id, Data>( configuration, persistence )
                        ),
                listeners );
    }

}
