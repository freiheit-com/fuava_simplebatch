package com.freiheit.fuava.simplebatch.fsjobs.downloader;

import java.util.List;

import com.freiheit.fuava.simplebatch.BatchJob;
import com.freiheit.fuava.simplebatch.fetch.Fetcher;
import com.freiheit.fuava.simplebatch.logging.ProcessingBatchListener;
import com.freiheit.fuava.simplebatch.logging.ProcessingItemListener;
import com.freiheit.fuava.simplebatch.persist.AbstractStringPersistenceAdapter;
import com.freiheit.fuava.simplebatch.persist.ControlFilePersistenceOutputInfo;
import com.freiheit.fuava.simplebatch.persist.Persistence;
import com.freiheit.fuava.simplebatch.persist.PersistenceAdapter;
import com.freiheit.fuava.simplebatch.persist.Persistences;
import com.freiheit.fuava.simplebatch.process.Processor;
import com.freiheit.fuava.simplebatch.result.ProcessingResultListener;

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
		private static final String LOG_NAME_BATCH = "ITEMS DOWNLOADED";
        private static final String LOG_NAME_ITEM = "ITEM";
		private final BatchJob.Builder<Id, Data> builder = BatchJob.builder();
        private Persistence<Id, Data, ?>  persistence;
        
        private Configuration configuration;

        public Builder() {

        }

        public Builder<Id, Data> setConfiguration( Configuration configuration ) {
            this.configuration = configuration;
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
                Processor<Id, Data> byIdsFetcher ) {
            builder.setProcessor( byIdsFetcher );
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
            setPersistence(persistenceAdapter);
            return this;
        }
        
        public Builder<Id, Data> setBatchFileWriterAdapter( PersistenceAdapter<List<Id>, List<Data>> persistenceAdapter ) {
            this.persistence = Persistences.controlledBatchFile(
            		configuration.getDownloadDirPath(),
            		configuration.getControlFileEnding(),
            		persistenceAdapter
        		);
            return this;
        }
        
        private void setPersistence(PersistenceAdapter<Id, Data> persistenceAdapter) {
            persistence = createControlledFilePersistence(persistenceAdapter);
        }

		private <I, O> Persistence<I, O, ControlFilePersistenceOutputInfo> createControlledFilePersistence(
				PersistenceAdapter<I, O> persistenceAdapter) {
			return Persistences.controlledFile(configuration.getDownloadDirPath(), configuration.getControlFileEnding(), persistenceAdapter);
		}

		
        public CtlDownloaderJob<Id, Data> build() {
            builder.addListener( new ProcessingBatchListener<Id, Data>(LOG_NAME_BATCH) );
            builder.addListener( new ProcessingItemListener<Id, Data>(LOG_NAME_ITEM) );
            if (persistence == null) {
            	setPersistence(new AbstractStringPersistenceAdapter<Id, Data>() {});
            }
            return new CtlDownloaderJob<Id, Data>(
                    builder.getProcessingBatchSize(),
                    builder.getFetcher(),
                    builder.getProcessor(),
                    this.configuration == null? new ConfigurationImpl(): this.configuration,
                    persistence,
                    builder.getListeners() );
        }

    }

    private CtlDownloaderJob(
            int processingBatchSize,
            Fetcher<Id> fetcher,
            Processor<Id, Data> processor,
            Configuration configuration,
            Persistence<Id, Data, ?> persistence,
            List<ProcessingResultListener<Id, Data>> listeners ) {
        super( processingBatchSize, fetcher, processor, persistence, listeners );
    }

}
