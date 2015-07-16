package com.freiheit.fuava.simplebatch.fsjobs.importer;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.freiheit.fuava.simplebatch.BatchJob;
import com.freiheit.fuava.simplebatch.fetch.FailsafeFetcherImpl;
import com.freiheit.fuava.simplebatch.fetch.Fetcher;
import com.freiheit.fuava.simplebatch.logging.ProcessingBatchListener;
import com.freiheit.fuava.simplebatch.logging.ProcessingItemListener;
import com.freiheit.fuava.simplebatch.persist.FilePersistence;
import com.freiheit.fuava.simplebatch.persist.Persistence;
import com.freiheit.fuava.simplebatch.persist.RetryingPersistence;
import com.freiheit.fuava.simplebatch.process.DelegatingSingleItemProcessor;
import com.freiheit.fuava.simplebatch.process.Processor;
import com.freiheit.fuava.simplebatch.process.Processors;
import com.freiheit.fuava.simplebatch.result.ProcessingResultListener;
import com.google.common.base.Function;
import com.google.common.base.Supplier;

/**
 * An importer that imports files from the file system, adhering to the control file protocol.
 * @author klas
 *
 * @param <ProcessedData>
 */
public class CtlImporterJob<Data>  extends BatchJob<ControlFile, Iterable<Data>> {


	public interface Configuration extends FilePersistence.Configuration {
		String getControlFileEnding();
		String getArchivedDirPath();
		String getProcessingDirPath();
		String getFailedDirPath();
	}

	public static final class ConfigurationImpl implements Configuration {

		private String downloadDirPath;
		private String archivedDirPath;
		private String processingDirPath;
		private String failedDirPath;
		private String controlFileEnding;

		@Override
		public String getDownloadDirPath() {
			return downloadDirPath;
		}

		public ConfigurationImpl setDownloadDirPath(String downloadDirPath) {
			this.downloadDirPath = downloadDirPath;
			return this;
		}

		@Override
		public String getArchivedDirPath() {
			return archivedDirPath;
		}

		public ConfigurationImpl setArchivedDirPath(String archivedDirPath) {
			this.archivedDirPath = archivedDirPath;
			return this;
		}

		@Override
		public String getFailedDirPath() {
			return failedDirPath;
		}

		public ConfigurationImpl setFailedDirPath(String failedDirPath) {
			this.failedDirPath = failedDirPath;
			return this;
		}

		@Override
		public String getProcessingDirPath() {
			return processingDirPath;
		}

		public ConfigurationImpl setProcessingDirPath(String processingDirPath) {
			this.processingDirPath = processingDirPath;
			return this;
		}

		public String getControlFileEnding() {
			return controlFileEnding;
		}

		public ConfigurationImpl setControlFileEnding(String controlFileEnding) {
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
		private int processingBatchSize;
		private Function<InputStream, Iterable<Data>> documentReader;
		private List<ProcessingResultListener<ControlFile, Iterable<Data>>> fileProcessingListeners = new ArrayList<>();

		private List<ProcessingResultListener<Data, Data>> contentProcessingListeners = new ArrayList<>();
		private Persistence<Data, Data, ?> contentPersistence;

		public Builder() {
		}

		public Builder<Data> setConfiguration(Configuration configuration) {
			this.configuration = configuration;
			return this;
		}


		/**
		 * The number of files to read (and subsequently persist) together  in one batch.
		 */
		public Builder<Data> setContentBatchSize(int processingBatchSize) {
			this.processingBatchSize = processingBatchSize;
			return this;
		}


		public Builder<Data> setFileInputStreamReader(Function<InputStream, Iterable<Data>> documentReader) {
			this.documentReader = documentReader;
			return this;
		}


		public <PersistenceResult> Builder<Data> setContentRetryableListPersistence(Function<List<Data>, List<PersistenceResult>> persistence) {
			contentPersistence = new RetryingPersistence<Data, Data, PersistenceResult>(persistence);
			return this;
		}

		public <PersistenceResult> Builder<Data> setContentPersistence(Persistence<Data, Data, PersistenceResult> persistence) {
			contentPersistence = persistence;
			return this;
		}


		public Builder<Data> addFileProcessingListener(ProcessingResultListener<ControlFile, Iterable<Data>> listener) {
			fileProcessingListeners.add(listener);
			return this;
		}


		public Builder<Data> addContentProcessingListener(ProcessingResultListener<Data, Data> listener) {
			contentProcessingListeners.add(listener);
			return this;
		}

		public CtlImporterJob<Data> build() {
			fileProcessingListeners.add( new ProcessingBatchListener<ControlFile, Iterable<Data>>(LOG_NAME_FILE_PROCESSING_BATCH) );
			fileProcessingListeners.add( new ProcessingItemListener<ControlFile, Iterable<Data>>(LOG_NAME_FILE_PROCESSING_ITEM) );

			contentProcessingListeners.add( new ProcessingBatchListener<Data, Data>(LOG_NAME_CONTENT_PROCESSING_BATCH) );
			contentProcessingListeners.add( new ProcessingItemListener<Data, Data>(LOG_NAME_CONTENT_PROCESSING_ITEM) );

			final Supplier<Iterable<ControlFile>> controlFileFetcher = new DirectoryFileFetcher<ControlFile>(
					this.configuration.getDownloadDirPath(), this.configuration.getControlFileEnding(),
					new ReadControlFileFunction()
					);

			final Processor<Iterable<Data>, Iterable<Data>> innerJobProcessor = new InnerJobProcessor<Data>(
					processingBatchSize, contentProcessingListeners, contentPersistence
					);
			final Function<File, Iterable<Data>> fileProcessorFunction = new FileToInputStreamFunction<>(this.documentReader);
			final Processor<File, Iterable<Data>> fileProcessor = new DelegatingSingleItemProcessor<File, Iterable<Data>>(fileProcessorFunction);
			final Processor<ControlFile, File> prepare = new PrepareControlledFile(configuration.getProcessingDirPath(), configuration.getDownloadDirPath());
			final Processor<ControlFile, Iterable<Data>> controlFileProcessor = Processors.compose(fileProcessor, prepare);
			final Processor<ControlFile, Iterable<Data>> processor = Processors.compose(innerJobProcessor, controlFileProcessor);
			
			final FileMovingPersistence<Iterable<Data>> persistence = new FileMovingPersistence<Iterable<Data>>(
					configuration.getProcessingDirPath(), 
					configuration.getArchivedDirPath(), 
					configuration.getFailedDirPath()
				);


			return new CtlImporterJob<Data>(
					1 /*process one file at a time, no use for batching*/, 
					new FailsafeFetcherImpl<ControlFile>(controlFileFetcher), 
					processor, 
					persistence, 
					fileProcessingListeners);
		}		
	}



	private CtlImporterJob(
			int processingBatchSize,
			Fetcher<ControlFile> fetcher,
			Processor<ControlFile, Iterable<Data>> processor,
			Persistence<ControlFile, Iterable<Data>, ?> persistence,
			List<ProcessingResultListener<ControlFile, Iterable<Data>>> listeners) {
		super(processingBatchSize, fetcher, processor, persistence, listeners);
		// TODO Auto-generated constructor stub
	}


}
