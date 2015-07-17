package com.freiheit.fuava.simplebatch.process;

import com.freiheit.fuava.simplebatch.BatchJob;
import com.freiheit.fuava.simplebatch.fetch.Fetchers;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;

final class InnerJobProcessor<Data> extends AbstractSingleItemProcessor<Iterable<Data>, Iterable<Data>> {
    private final BatchJob.Builder<Data, Data> builder;

    public InnerJobProcessor(
            BatchJob.Builder<Data, Data> builder
            ) {
        this.builder = builder;
    }

    @Override
    public final Result<Iterable<Data>, Iterable<Data>> processItem(Iterable<Data> inputs) {

        final BatchJob<Data, Data> job = builder.setFetcher(Fetchers.iterable(inputs)).build();
        final ResultStatistics statistics = job.run();

        if (statistics.isAllFailed()) {
            return Result.failed(inputs, "Processing of all Items failed. Please check the log files.");
        }

        return Result.success(inputs, inputs);
    }

}