package com.freiheit.fuava.simplebatch.fsjobs.importer;

import java.util.List;

import com.freiheit.fuava.simplebatch.BatchJob;
import com.freiheit.fuava.simplebatch.persist.Persistence;
import com.freiheit.fuava.simplebatch.process.IdentityProcessor;
import com.freiheit.fuava.simplebatch.process.SingleItemProcessor;
import com.freiheit.fuava.simplebatch.result.ProcessingResultListener;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;

final class InnerJobProcessor<Data> extends SingleItemProcessor<Iterable<Data>, Iterable<Data>> {
	private final int processingBatchSize;
	private final List<ProcessingResultListener<Data, Data>> contentProcessingListeners;
	private final Persistence<Data, Data, ?> contentPersistence;

	public InnerJobProcessor(
			int processingBatchSize,
			List<ProcessingResultListener<Data, Data>> contentProcessingListeners,
			Persistence<Data, Data, ?> contentPersistence
			) {
		this.processingBatchSize = processingBatchSize;
		this.contentProcessingListeners = contentProcessingListeners;
		this.contentPersistence = contentPersistence;
	}

	@Override
	public final Result<Iterable<Data>, Iterable<Data>> processItem(Iterable<Data> inputs) {

		final BatchJob.Builder<Data, Data> builder = BatchJob.<Data, Data>builder()
				.setProcessingBatchSize(processingBatchSize)
				.setFetcher(inputs)
				.setProcessor(new IdentityProcessor<Data>())
				.setPersistence(contentPersistence);

		for (ProcessingResultListener<Data, Data> l: contentProcessingListeners) {
			builder.addListener(l);
		}

		final BatchJob<Data, Data> job = builder.build();
		final ResultStatistics<Data,Data> statistics = job.run();

		if (statistics.isAllFailed()) {
			return Result.failed(inputs, "Processing of all Items failed. Please check the log files.");
		}

		return Result.success(inputs, inputs);
	}

}