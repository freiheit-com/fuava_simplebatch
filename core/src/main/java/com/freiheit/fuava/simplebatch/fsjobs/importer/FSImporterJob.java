package com.freiheit.fuava.simplebatch.fsjobs.importer;

import java.io.File;
import java.util.List;
import java.util.Map;

import com.freiheit.fuava.simplebatch.BatchJob;
import com.freiheit.fuava.simplebatch.fetch.Fetcher;
import com.freiheit.fuava.simplebatch.persist.Persistence;
import com.freiheit.fuava.simplebatch.process.Processor;
import com.freiheit.fuava.simplebatch.result.ProcessingResultListener;
import com.google.common.base.Function;
import com.google.common.base.Supplier;

/**
 * An importer that imports files from the file system, adhering to the control file protocol.
 * @author klas
 *
 * @param <Output>
 */
public class FSImporterJob<Output>  extends BatchJob<File, Output> {


	public static final class Builder<Output> {
		private final BatchJob.Builder<File, Output> builder = BatchJob.builder();


		public Builder() {

		}
		
		public Builder<Output> setProcessingBatchSize(
				int processingBatchSize) {
			builder.setProcessingBatchSize(processingBatchSize);
			return this;
		}


		public Builder<Output> setFetcher(
				Fetcher<File> idsFetcher) {
			builder.setFetcher(idsFetcher);
			return this;
		}


		public Builder<Output> setFetcher(
				Iterable<File> idsFetcher) {
			builder.setFetcher(idsFetcher);
			return this;
		}


		public Builder<Output> setFetcher(
				Supplier<Iterable<File>> idsFetcher) {
			builder.setFetcher(idsFetcher);
			return this;
		}


		public Builder<Output> setProcessor(
				Processor<File, Output> byIdsFetcher) {
			builder.setProcessor(byIdsFetcher);
			return this;
		}


		public Builder<Output> setRetryableProcessor(
				Function<List<File>, Map<File, Output>> retryableFunction) {
			builder.setRetryableProcessor(retryableFunction);
			return this;
		}


		public Builder<Output> setRetryableListProcessor(
				Function<List<File>, List<Output>> retryableFunction) {
			builder.setRetryableListProcessor(retryableFunction);
			return this;
		}


		public Builder<Output> addListener(
				ProcessingResultListener<File, Output> listener) {
			builder.addListener(listener);
			return this;
		}


		public Builder<Output> removeListener(
				ProcessingResultListener<File, Output> listener) {
			builder.removeListener(listener);
			return this;
		}



		public FSImporterJob<Output> build() {
			return new FSImporterJob<Output>(
					builder.getProcessingBatchSize(), 
					builder.getFetcher(), 
					builder.getProcessor(), 
					builder.getPersistence(),
					builder.getListeners()
			);
		}		
	}
	
	
	protected FSImporterJob(
			int processingBatchSize, 
			Fetcher<File> fetcher,
			Processor<File, Output> processor,
			Persistence<File, Output> persistence,
			List<ProcessingResultListener<File, Output>> listeners
	) {
		super(processingBatchSize, fetcher, processor, persistence, listeners);
	}

	
	
}
