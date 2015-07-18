package com.freiheit.fuava.simplebatch.fsjobs.downloader;

import java.util.List;

import com.freiheit.fuava.simplebatch.BatchJob;
import com.freiheit.fuava.simplebatch.fetch.Fetcher;
import com.freiheit.fuava.simplebatch.logging.BatchStatisticsLoggingListener;
import com.freiheit.fuava.simplebatch.logging.ItemProgressLoggingListener;
import com.freiheit.fuava.simplebatch.processor.AbstractStringFileWriterAdapter;
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
        private static final String LOG_NAME_BATCH = "ITEMS DOWNLOADED";
        private static final String LOG_NAME_ITEM = "ITEM";
        private final BatchJob.Builder<Id, Data> builder = BatchJob.builder();
        private Processor<Id, Data, ?>  fileWriter;

        private Configuration configuration;

        public Builder() {

        }

        public Builder<Id, Data> setConfiguration( Configuration configuration ) {
            this.configuration = configuration;
            return this;
        }

        public Builder<Id, Data>  setDescription(String description) {
            builder.setDescription(description);
            return this;
        }


        /**
         * The amount of items from the fetch stage that are put together in a list and passed on to the "Downloader" stage.
         * If your downloader supports batch fetching, you can use this setting to control the amount of items in one batch.
         */
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
         * Uses the Ids to download the data.
         */
        public Builder<Id, Data> setDownloader(
                Processor<Id, Id, Data> byIdsFetcher ) {
            builder.setPersistence( byIdsFetcher );
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

        public Builder<Id, Data> setFileWriterAdapter( FileWriterAdapter<Id, Data> persistenceAdapter ) {
            this.fileWriter = Processors.controlledFileWriter(configuration.getDownloadDirPath(), configuration.getControlFileEnding(), persistenceAdapter);
            return this;
        }

        public Builder<Id, Data> setBatchFileWriterAdapter( FileWriterAdapter<List<Id>, List<Data>> persistenceAdapter ) {
            this.fileWriter = Processors.controlledBatchFileWriter(
                    configuration.getDownloadDirPath(),
                    configuration.getControlFileEnding(),
                    persistenceAdapter
                    );
            return this;
        }

        public CtlDownloaderJob<Id, Data> build() {
            builder.addListener( new BatchStatisticsLoggingListener<Id, Data>(LOG_NAME_BATCH) );
            builder.addListener( new ItemProgressLoggingListener<Id, Data>(LOG_NAME_ITEM) );
            if (fileWriter == null) {
                setFileWriterAdapter(new AbstractStringFileWriterAdapter<Id, Data>() {});
                Preconditions.checkNotNull(fileWriter);
            }
            Processor<Id, Id, ?> p = Processors.compose(fileWriter, builder.getPersistence());
            return new CtlDownloaderJob<Id, Data>(
                    builder.getDescription(),
                    builder.getProcessingBatchSize(),
                    builder.getFetcher(),
                    this.configuration == null? new ConfigurationImpl(): this.configuration,
                            p,
                            builder.getListeners() );
        }

    }

    private CtlDownloaderJob(
            String description,
            int processingBatchSize,
            Fetcher<Id> fetcher,
            Configuration configuration,
            Processor<Id, Id, ?> persistence,
            List<ProcessingResultListener<Id, Data>> listeners ) {
        super( description, processingBatchSize, fetcher, persistence, listeners );
    }

}
