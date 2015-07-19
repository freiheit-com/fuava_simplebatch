package com.freiheit.fuava.simplebatch;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.CheckReturnValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freiheit.fuava.simplebatch.fetch.FetchedItem;
import com.freiheit.fuava.simplebatch.fetch.Fetcher;
import com.freiheit.fuava.simplebatch.processor.Processor;
import com.freiheit.fuava.simplebatch.result.DelegatingProcessingResultListener;
import com.freiheit.fuava.simplebatch.result.ProcessingResultListener;
import com.freiheit.fuava.simplebatch.result.Result;
import com.freiheit.fuava.simplebatch.result.ResultStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * Downloads - or more generally, processes - data in two stages, via iterables.
 *
 * The output of the first stage is used as input for the second stage.
 *
 * A typical usecase would be to fetch Ids or URLs of the data to download in
 * the first stage ('source'), and then to fetch the data in the second stage
 * ('process'), persisting the data and the progress information in the third
 * stage 'sink'.
 *
 * The downloader uses iterables and does the stage 2 downloading in batches, so
 * you can provide iterables that stream over some data source, efficiently
 * processing huge amounts of data.
 *
 * @author Klas Kalass <klas.kalass@freiheit.com>
 *
 * @param <Input>
 *            The data fetched in stage one
 * @param <Output>
 *            The data fetched instage two
 */
public class BatchJob<Input, Output> {
    static final Logger LOG = LoggerFactory.getLogger( BatchJob.class );

    public static class Builder<Input, Output> {
        private int processingBatchSize = 1000;
        private Fetcher<Input> fetcher;
        private Processor<FetchedItem<Input>, Input, Output> persistence;

        private final ArrayList<ProcessingResultListener<Input, Output>> listeners = new ArrayList<ProcessingResultListener<Input, Output>>();
        private String description;

        public Builder() {
        }

        public int getProcessingBatchSize() {
            return processingBatchSize;
        }

        public Builder<Input, Output> setProcessingBatchSize( int processingBatchSize ) {
            this.processingBatchSize = processingBatchSize;
            return this;
        }

        public Builder<Input, Output> setFetcher( Fetcher<Input> idsFetcher ) {
            this.fetcher = idsFetcher;
            return this;
        }

        public Fetcher<Input> getFetcher() {
            return fetcher;
        }


        public Builder<Input, Output> setPersistence( Processor<FetchedItem<Input>, Input, Output> writer ) {
            this.persistence = writer;
            return this;
        }

        public Processor<FetchedItem<Input>, Input, Output> getPersistence() {
            return persistence;
        }

        public Builder<Input, Output> addListener(ProcessingResultListener<Input, Output> listener) {
            this.listeners.add(listener);
            return this;
        }

        public Builder<Input, Output> removeListener(ProcessingResultListener<Input, Output> listener) {
            this.listeners.remove(listener);
            return this;
        }

        public ArrayList<ProcessingResultListener<Input, Output>> getListeners() {
            return listeners;
        }

        public Builder<Input, Output> setDescription(String desc) {
            this.description = desc;
            return this;
        }

        public BatchJob<Input, Output> build() {
            return new BatchJob<Input, Output>(description, processingBatchSize, fetcher, persistence, listeners);
        }


        public String getDescription() {
            return description;
        }

    }

    private final int processingBatchSize;
    private final Fetcher<Input> fetcher;
    private final Processor<FetchedItem<Input>, Input, Output> persistence;

    private final List<ProcessingResultListener<Input, Output>> listeners;
    private final String description;

    protected BatchJob(
            String description,
            int processingBatchSize,
            Fetcher<Input> fetcher,
            Processor<FetchedItem<Input>, Input, Output> persistence,
            List<ProcessingResultListener<Input, Output>> listeners
            ) {
        this.description = description;
        this.processingBatchSize = processingBatchSize;
        this.fetcher = fetcher;
        this.persistence = persistence;
        this.listeners = ImmutableList.copyOf(listeners);
    }

    public static <Input, Output> Builder<Input, Output> builder() {
        return new Builder<Input, Output>();
    }

    @CheckReturnValue
    public final ResultStatistics run() {
        ResultStatistics.Builder<Input, Output> resultBuilder = ResultStatistics.builder();

        DelegatingProcessingResultListener<Input, Output> listeners = new DelegatingProcessingResultListener<Input, Output>(
                ImmutableList.<ProcessingResultListener<Input, Output>>builder().add(resultBuilder).addAll(this.listeners).build()
                );

        listeners.onBeforeRun(this.description);

        final Iterable<Result<FetchedItem<Input>, Input>> sourceIterable = fetcher.fetchAll();

        for ( List<Result<FetchedItem<Input>, Input>> sourceResults : Iterables.partition( sourceIterable, processingBatchSize ) ) {

            listeners.onFetchResults(sourceResults);

            Iterable<? extends Result<FetchedItem<Input>, Output>> persistResults = persistence.process( sourceResults );

            listeners.onProcessingResults(persistResults);
        }

        listeners.onAfterRun();
        resultBuilder.setListenerDelegationFailures(listeners.hasDelegationFailures());


        // TODO: persist the state of the downloader (offset or downloader), so it can be
        //       provided the next time
        //idsDownloader.getWriteableState();
        return resultBuilder.build();
    }
}