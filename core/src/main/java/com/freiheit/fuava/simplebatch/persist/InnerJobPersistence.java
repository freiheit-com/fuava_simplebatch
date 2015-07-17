package com.freiheit.fuava.simplebatch.persist;

import com.freiheit.fuava.simplebatch.BatchJob;
import com.freiheit.fuava.simplebatch.fetch.Fetchers;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;
import com.google.common.base.Function;

final class InnerJobPersistence<Input, Data> extends AbstractSingleItemPersistence<Input, Iterable<Data>, Iterable<Data>> {
    private final Function<Input, String> jobDescriptionFunc;
    private final BatchJob.Builder<Data, Data> builder;

    public InnerJobPersistence(
            Function<Input, String> jobDescriptionFunc,
            BatchJob.Builder<Data, Data> builder
            ) {
        this.builder = builder;
        this.jobDescriptionFunc = jobDescriptionFunc;
    }

    @Override
    public Result<Input, Iterable<Data>> persistItem(Result<Input, Iterable<Data>> previous) {
        Input i = previous.getInput();
        Iterable<Data> output = previous.getOutput();
        String desc = jobDescriptionFunc.apply(i);
        final BatchJob<Data, Data> job = builder.setFetcher(Fetchers.iterable(output)).setDescription(desc).build();
        final ResultStatistics statistics = job.run();

        if (statistics.isAllFailed()) {
            return Result.failed(i, "Processing of all Items failed. Please check the log files.");
        }

        return Result.success(i, output);
    }

}